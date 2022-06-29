import dill
import pandas as pd
from typing import Dict, List, Optional

import sqlalchemy

from .utils import unpack_jsonai_old_args, load_predictor

import lightwood
from lightwood.api.types import JsonAI
from lightwood.api.high_level import json_ai_from_problem, predictor_from_code, code_from_json_ai, ProblemDefinition

from .join_utils import get_ts_join_input
from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.utils import recur_get_conditionals, get_aliased_columns, get_join_input, default_data_gather, get_model_name
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.interfaces.model.learn_process import brack_to_mod, rep_recur
from mindsdb.utilities.config import Config

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Join, BinaryOperation, Identifier, Constant, Select, OrderBy
from mindsdb_sql.parser.dialects.mindsdb import (
    RetrainPredictor,
    CreatePredictor,
    DropPredictor
)


MDB_CURRENT_HANDLERS = {
    'test_handler': MySQLHandler('test_handler', **{"connection_data": {
        "host": "localhost",
        "port": "3306",
        "user": "root",
        "password": "root",
        "database": "test",
        "ssl": False
    }})
}  # TODO: remove once hooked to mindsdb handler controller


class LightwoodHandler(PredictiveHandler):

    name = 'lightwood'

    def __init__(self, name):
        """ Lightwood AutoML integration """  # noqa
        super().__init__(name)
        self.storage = None
        self.parser = parse_sql
        self.dialect = 'mindsdb'
        self.handler_dialect = 'mysql'

        self.lw_dtypes_to_sql = {
            "integer": sqlalchemy.Integer,
            "float": sqlalchemy.Float,
            "quantity": sqlalchemy.Float,
            "binary": sqlalchemy.Text,
            "categorical": sqlalchemy.Text,
            "tags": sqlalchemy.Text,
            "date": sqlalchemy.DateTime,
            "datetime": sqlalchemy.DateTime,
            "short_text": sqlalchemy.Text,
            "rich_text": sqlalchemy.Text,
            "num_array": sqlalchemy.Text,
            "cat_array": sqlalchemy.Text,
            "num_tsarray": sqlalchemy.Text,
            "cat_tsarray": sqlalchemy.Text,
            "empty": sqlalchemy.Text,
            "invalid": sqlalchemy.Text,
        }  # image, audio, video not supported
        self.lw_dtypes_overrides = {
            'original_index': sqlalchemy.Integer,
            'confidence': sqlalchemy.Float,
            'lower': sqlalchemy.Float,
            'upper': sqlalchemy.Float
        }

    def connect(self, **kwargs) -> Dict[str, int]:
        """ Setup storage handler and check lightwood version """  # noqa
        self.storage = SqliteStorageHandler(context=self.name, config=kwargs['config'])  # TODO non-KV storage handler?
        return self.check_connection()

    def check_connection(self) -> Dict[str, int]:
        try:
            import lightwood
            year, major, minor, hotfix = lightwood.__version__.split('.')
            assert int(year) > 22 or (int(year) == 22 and int(major) >= 4)
            print("Lightwood OK!")
            return {'status': '200'}
        except AssertionError as e:
            print("Cannot import lightwood!")
            return {'status': '503', 'error': e}

    def get_tables(self) -> List:
        """ Returns list of model names (that have been succesfully linked with CREATE PREDICTOR) """  # noqa
        models = self.storage.get('models')
        return list(models.keys()) if models else []

    def describe_table(self, table_name: str) -> Dict:
        """ For getting standard info about a table. e.g. data types """  # noqa
        if table_name not in self.get_tables():
            print("Table not found.")
            return {}
        return self.storage.get('models')[table_name]['jsonai']

    def native_query(self, query: str) -> Optional[object]:
        statement = self.parser(query, dialect=self.dialect)

        if type(statement) == CreatePredictor:
            model_name = statement.name.parts[-1]

            if model_name in self.get_tables():
                raise Exception("Error: this model already exists!")

            target = statement.targets[0].parts[-1]
            params = { 'target': target }
            if statement.order_by:
                params['timeseries_settings'] = {
                    'is_timeseries': True,
                    'order_by': [str(col) for col in statement.order_by],
                    'group_by': [str(col) for col in statement.group_by],
                    'window': int(statement.window),
                    'horizon': int(statement.horizon),
                }

            json_ai_override = statement.using if statement.using else {}
            unpack_jsonai_old_args(json_ai_override)

            # get training data from other integration
            handler = MDB_CURRENT_HANDLERS[str(statement.integration_name)]  # TODO import from mindsdb init
            handler_query = self.parser(statement.query_str, dialect=self.handler_dialect)
            df = default_data_gather(handler, handler_query)

            json_ai_keys = list(lightwood.JsonAI.__dict__['__annotations__'].keys())
            json_ai = json_ai_from_problem(df, ProblemDefinition.from_dict(params)).to_dict()
            json_ai_override = brack_to_mod(json_ai_override)
            rep_recur(json_ai, json_ai_override)
            json_ai = JsonAI.from_dict(json_ai)

            code = code_from_json_ai(json_ai)
            predictor = predictor_from_code(code)
            predictor.learn(df)

            all_models = self.storage.get('models')
            serialized_predictor = dill.dumps(predictor)
            payload = {
                'code': code,
                'jsonai': json_ai,
                'stmt': statement,
                'predictor': serialized_predictor,
            }
            if all_models is not None:
                all_models[model_name] = payload
            else:
                all_models = {model_name: payload}
            self.storage.set('models', all_models)

        elif type(statement) == RetrainPredictor:
            model_name = statement.name.parts[-1]
            if model_name not in self.get_tables():
                raise Exception("Error: this model does not exist, so it can't be retrained. Train a model first.")

            all_models = self.storage.get('models')
            original_stmt = all_models[model_name]['stmt']

            handler = MDB_CURRENT_HANDLERS[str(original_stmt.integration_name)]  # TODO import from mindsdb init
            handler_query = self.parser(original_stmt.query_str, dialect=self.handler_dialect)
            df = default_data_gather(handler, handler_query)

            predictor = load_predictor(all_models[model_name], model_name)
            predictor.adjust(df)
            all_models[model_name]['predictor'] = dill.dumps(predictor)
            self.storage.set('models', all_models)

        elif type(statement) == DropPredictor:
            to_drop = statement.name.parts[-1]
            all_models = self.storage.get('models')
            del all_models[to_drop]
            self.storage.set('models', all_models)

        else:
            raise Exception(f"Query type {type(statement)} not supported")

    def query(self, query) -> dict:
        model_name, _, _ = get_model_name(self, query)
        model = self._get_model(model_name)
        values = recur_get_conditionals(query.where.args, {})
        df = pd.DataFrame.from_dict(values)
        df = self._call_predictor(df, model)
        return {'data_frame': df}

    def join(self, stmt, data_handler: BaseHandler, into: Optional[str] = None) -> pd.DataFrame:
        """
        Batch prediction using the output of a query passed to a data handler as input for the model.
        """  # noqa
        model_name, model_alias, model_side = get_model_name(self, stmt)
        data_side = 'right' if model_side == 'left' else 'left'
        model = self._get_model(model_name)
        is_ts = model.problem_definition.timeseries_settings.is_timeseries

        if not is_ts:
            model_input = get_join_input(stmt, model, [model_name, model_alias], data_handler, data_side)
        else:
            model_input = get_ts_join_input(stmt, model, data_handler, data_side)

        # get model output and rename columns
        predictions = self._call_predictor(model_input, model)
        model_input.columns = get_aliased_columns(list(model_input.columns), model_alias, stmt.targets, mode='input')
        predictions.columns = get_aliased_columns(list(predictions.columns), model_alias, stmt.targets, mode='output')

        if into:
            try:
                dtypes = {}
                for col in predictions.columns:
                    if model.dtype_dict.get(col, False):
                        dtypes[col] = self.lw_dtypes_to_sql.get(col, sqlalchemy.Text)
                    else:
                        dtypes[col] = self.lw_dtypes_overrides.get(col, sqlalchemy.Text)

                data_handler.select_into(into, predictions, dtypes=dtypes)
            except Exception as e:
                print("Error when trying to store the JOIN output in data handler.")

        return predictions

    def _get_model(self, model_name):
        predictor_dict = self._get_model_info(model_name)
        predictor = load_predictor(predictor_dict, model_name)
        return predictor

    def _get_model_info(self, model_name):
        """ Returns a dictionary with three keys: 'jsonai', 'predictor' (serialized), and 'code'. """  # noqa
        return self.storage.get('models')[model_name]

    def _call_predictor(self, df, predictor):
        predictions = predictor.predict(df)
        if 'original_index' in predictions.columns:
            predictions = predictions.sort_values(by='original_index')
        return df.join(predictions)
