from typing import Optional, Any, Dict

import dask
import dill
import sqlalchemy
import pandas as pd
from ludwig.automl import auto_train

from mindsdb_sql import parse_sql
from mindsdb.utilities.log import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import BinaryOperation, Identifier, Constant, Select, Show, Star, NativeQuery
from mindsdb.integrations.utilities.utils import get_join_input, recur_get_conditionals, get_aliased_columns, default_data_gather, make_sql_session, get_where_data
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse,
    HandlerResponse,
    RESPONSE_TYPE
)
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records
)
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery
from mindsdb_sql.parser.dialects.mindsdb import (
    CreatePredictor,
    RetrainPredictor,
    DropPredictor,
)


class LudwigHandler(PredictiveHandler):

    name = 'ludwig'
    predictor_cache: Dict[str, Dict[str, Any]]

    def __init__(self, name, **kwargs):
        """
        Handler to create and use Ludwig AutoML models from MindsDB.
        """  # noqa
        super().__init__(name)
        self.config = Config()
        self.predictor_cache = {}
        storage_config = kwargs.get('storage_config', {'name': "ludwig_handler_storage"})
        self.storage = SqliteStorageHandler(context={}, config=storage_config)
        try:
            self.storage.get('models')
        except KeyError:
            self.storage.set('models', {})

        self.parser = parse_sql
        self.dialect = 'mindsdb'
        self.handler_dialect = 'mysql'

        self.handler_controller = kwargs.get('handler_controller')
        self.fs_store = kwargs.get('fs_store')
        self.company_id = kwargs.get('company_id')
        self.model_controller = WithKWArgsWrapper(
            ModelController(),
            company_id=self.company_id
        )

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
        }

    def check_connection(self, **kwargs) -> HandlerStatusResponse:
        """ Setup storage and check whether Ludwig is available. """  # noqa
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

    def get_tables(self) -> HandlerResponse:
        """ Returns name list of trained models.  """  # noqa
        models = self.storage.get('models', [])
        if models:
            df = pd.DataFrame(
                list(models.keys()) if models else [],
                columns=['table_name']
            )
        else:
            df = pd.DataFrame(columns=['table_name'])

        return HandlerResponse(RESPONSE_TYPE.TABLE, df)

    def get_columns(self, table_name: str) -> HandlerResponse:
        """ For any given model, return the input data types. """  # noqa
        try:
            model = self.storage.get('models', {}).get(table_name, {}).get('model', None)
            if not model:
                df = pd.DataFrame(columns=['COLUMN_NAME', 'DATA_TYPE'])
            else:
                model = dill.loads(model)
                cfg = model.config
                df = pd.DataFrame(
                    [v for v in cfg.values()],
                    columns=[k for k in cfg.keys()]
                )
            r = HandlerResponse(RESPONSE_TYPE.TABLE, df)
        except Exception as e:
            log.error(f"Could not get columns for model {table_name}, error: {e}")
            r = HandlerResponse(RESPONSE_TYPE.ERROR)
        return r

    def native_query(self, query: Any) -> HandlerResponse:
        statement = self.parser(query, dialect=self.dialect)
        return self.query(statement)

    def query(self, query: ASTNode) -> HandlerResponse:
        """old query body
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
        """
        if type(query) == CreatePredictor:
            self._learn(query)

        elif type(query) == RetrainPredictor:
            msg = 'Warning: retraining Ludwig models is not yet supported!'  # TODO: restore this
            return HandlerResponse(RESPONSE_TYPE.ERROR, error_message=msg)

        elif type(query) == DropPredictor:
            to_drop = query.name.parts[-1]
            models = self.storage.get('models')
            if models:
                del models[to_drop]
                self.storage.set('models', models)
            else:
                raise Exception(f"Can't drop non-existent model {to_drop}")

        # elif type(query) == Select:
        #     model_name = query.from_table.parts[-1]
        #     where_data = get_where_data(query.where)
        #     predictions = self.predict(model_name, where_data)
        #     return HandlerResponse(
        #         RESPONSE_TYPE.TABLE,
        #         data_frame=pd.DataFrame(predictions)
        #     )
        else:
            raise Exception(f"Query type {type(query)} not supported")
        
        return HandlerResponse(RESPONSE_TYPE.OK)

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

    @mark_process(name='learn')
    def _learn(self, statement):
        model_name = statement.name.parts[-1]

        if model_name in self.get_tables().data_frame.values:
            raise Exception("Error: this model already exists!")

        target = statement.targets[0].parts[-1]
        if statement.order_by:
            raise Exception("Ludwig handler does not support time series tasks yet!")

        # TODO: potentially abstract this into a common utility?
        # get data from integration  # TODO: custom dialect?
        integration_name = statement.integration_name.parts[0]
        query = Select(
            targets=[Star()],
            from_table=NativeQuery(
                integration=Identifier(integration_name),
                query=statement.query_str,
            )
        )
        sql_session = make_sql_session(self.company_id, ml_handler=self.name)
        sqlquery = SQLQuery(query, session=sql_session)
        df = sqlquery.fetch(view='dataframe')['result']

        results = auto_train(
            dataset=df,
            target=target,
            # TODO: add and enable custom values via SQL (mindful of local vs cloud) for these params:
            tune_for_memory=False,
            time_limit_s=120,
            # output_directory='./',
            user_config={'hyperopt': {'executor': {'gpu_resources_per_trial': 0, 'num_samples': 3}}},  # no GPU for now
            # random_seed=42,
            # use_reference_config=False,
            # kwargs={}
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
        log.info(f'Ludwig model {model_name} has finished training.')

    @mark_process(name='predict')
    def predict(self, model_name, data):
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        predictor_record = get_model_record(company_id=self.company_id, name=model_name)
        if predictor_record is None:
            return HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{model_name}' does not exists!"
            )

        fs_name = f'predictor_{self.company_id}_{predictor_record.id}'
        model_data = self.model_controller.get_model_data(predictor_record=predictor_record)

        return df