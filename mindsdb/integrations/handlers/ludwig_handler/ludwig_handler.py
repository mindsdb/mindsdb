import requests
from datetime import datetime
from typing import Dict, List, Optional, Any

import dask
import dill
import sqlalchemy
import pandas as pd
import ludwig as lw
from ludwig.api import LudwigModel
from ludwig.automl import auto_train

from mindsdb_sql import parse_sql
from mindsdb.utilities.log import log
from mindsdb.utilities.config import Config
from mindsdb_sql.parser.ast import Join, Select, Identifier, Constant, Star
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.utils import get_join_input, recur_get_conditionals, get_aliased_columns, default_data_gather
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler, DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse,
    HandlerResponse,
    RESPONSE_TYPE
)
from mindsdb_sql.parser.dialects.mindsdb import (
    CreatePredictor,
    RetrainPredictor,
    DropPredictor,
)


class LudwigHandler(PredictiveHandler):

    type = 'ludwig'

    def __init__(self, name):
        """
        Handler to create and use Ludwig AutoML models from MindsDB.
        """  # noqa
        super().__init__(name)
        self.storage = None
        self.parser = parse_sql
        self.dialect = 'mindsdb'
        self.handler_dialect = 'mysql'
        self.dtypes_to_sql = {
            "Number": sqlalchemy.Integer,
            "Binary": sqlalchemy.Text,
            "Category": sqlalchemy.Text,
            "Bag": sqlalchemy.Text,
            "Set": sqlalchemy.Text,
            "Date": sqlalchemy.DateTime,
            "Text": sqlalchemy.Text,
            "H3": sqlalchemy.Text,
            "Sequence": sqlalchemy.Text,
            "Vector": sqlalchemy.Text,
        }  # TODO audio, image?
        self.is_connected = False

    def connect(self, **kwargs) -> HandlerStatusResponse:
        """ Setup storage and check whether Ludwig is available. """  # noqa
        self.storage = SqliteStorageHandler(context=self.name, config=kwargs['config'])
        result = HandlerStatusResponse(False)
        try:
            import ludwig as lw
            from ludwig.api import LudwigModel
            from ludwig.automl import auto_train
            result.success = True
            self.is_connected = True
        except ImportError as e:
            log.error(f'Error importing Ludwig, {e}!')
            result.error_message = str(e)
        return result

    def disconnect(self):
        self.is_connected = False
        return

    def check_connection(self) -> HandlerStatusResponse:
        return HandlerResponse(self.is_connected)

    def get_tables(self) -> HandlerResponse:
        """ Returns name list of trained models.  """  # noqa
        models = self.storage.get('models')
        r = HandlerResponse(
            RESPONSE_TYPE.TABLE,
            pd.DataFrame(
                list(models.keys()) if models else [],
                columns=['model_name']
            )
        )
        return r

    def get_columns(self, table_name: str) -> HandlerResponse:
        """ For any given model, return the input data types. """  # noqa
        try:
            model = dill.loads(self.storage.get('models').get(table_name)['model'])
            if not model:
                model = LudwigModel()
            cfg = model.config  # json-like
            r = HandlerResponse(
                RESPONSE_TYPE.TABLE,
                pd.DataFrame(
                    [v for v in cfg.values()],
                    columns=[k for k in cfg.keys()]
                )
            )
        except Exception as e:
            log.error(f"Could not get columns for model {table_name}, error: {e}")
            r = HandlerResponse(RESPONSE_TYPE.ERROR)
        return r

    def native_query(self, query: Any) -> HandlerResponse:
        statement = self.parser(query, dialect=self.dialect)
        r = HandlerResponse(True)

        if type(statement) == CreatePredictor:
            model_name = statement.name.parts[-1]

            if model_name in self.get_tables().data_frame.values:
                raise Exception("Error: this model already exists!")

            target = statement.targets[0].parts[-1]
            if statement.order_by:
                raise Exception("Ludwig handler does not support time series tasks yet!")

            # get training data from other integration
            handler = MDB_CURRENT_HANDLERS[str(statement.integration_name)]  # TODO import from mindsdb init
            handler_query = self.parser(statement.query_str, dialect=self.handler_dialect)
            df = default_data_gather(handler, handler_query)

            grace_period = 72
            time_budget = 120
            results = auto_train(
                dataset=df,
                target=target,
                time_limit_s=max(grace_period, time_budget),  # TODO customizable (also, is grace period fixed?)
                tune_for_memory=False
            )
            model = results.best_model

            all_models = self.storage.get('models')
            payload = {
                'stmt': statement,
                'model': dill.dumps(model),
            }
            if all_models is not None:
                all_models[model_name] = payload
            else:
                all_models = {model_name: payload}
            self.storage.set('models', all_models)

        elif type(statement) == RetrainPredictor:
            log.warning('Warning: retraining Ludwig models is not yet supported!')  # TODO: restore this
            return r

        elif type(statement) == DropPredictor:
            to_drop = statement.name.parts[-1]
            models = self.storage.get('models')
            if models:
                del models[to_drop]
                self.storage.set('models', models)
            else:
                raise Exception(f"Can't drop non-existent model {to_drop}")

        else:
            raise Exception(f"Query type {type(statement)} not supported")
        
        return r

    def query(self, query: ASTNode) -> HandlerResponse:
        values = recur_get_conditionals(query.where.args, {})
        model_name, _, _ = self._get_model_name(query)
        model = self._get_model(model_name)
        df = pd.DataFrame.from_dict(values)
        df = self._call_model(df, model)
        r = HandlerResponse(
            RESPONSE_TYPE.TABLE,
            df
        )
        return r

    def join(self, stmt, data_handler, into: Optional[str]) -> HandlerResponse:
        """
        Batch prediction using the output of a query passed to a data handler as input for the model.
        """  # noqa

        model_name, model_alias, model_side = self._get_model_name(stmt)
        data_side = 'right' if model_side == 'left' else 'left'
        model = self._get_model(model_name)
        model_input = get_join_input(stmt, model, [model_name, model_alias], data_handler, data_side)

        # get model output and rename columns
        predictions = self._call_model(model_input, model)
        model_input.columns = get_aliased_columns(list(model_input.columns), model_alias, stmt.targets, mode='input')
        predictions.columns = get_aliased_columns(list(predictions.columns), model_alias, stmt.targets, mode='output')

        if into:
            try:
                dtypes = {}
                for col in predictions.columns:
                    if model.dtype_dict.get(col, False):
                        dtypes[col] = self.dtypes_to_sql.get(col, sqlalchemy.Text)

                data_handler.select_into(into, predictions, dtypes=dtypes)
            except Exception as e:
                print("Error when trying to store the JOIN output in data handler.")

        r = HandlerResponse(
            RESPONSE_TYPE.TABLE,
            predictions
        )
        return r

    def _get_model(self, model_name):
        model = dill.loads(self.storage.get('models')[model_name]['model'])
        return model

    def _call_model(self, df, model):
        predictions = dask.compute(model.predict(df)[0])[0]
        predictions.columns = ['prediction']
        joined = df.join(predictions)
        return joined
