import os
import sys
import dill
import traceback
from datetime import datetime
from typing import Dict, List, Optional

import sqlalchemy
import pandas as pd
import lightwood
from lightwood.api.types import JsonAI
from lightwood.api.high_level import json_ai_from_problem, predictor_from_code, code_from_json_ai, ProblemDefinition
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Join, BinaryOperation, Identifier, Constant, Select, OrderBy, Show
from mindsdb_sql.parser.dialects.mindsdb import (
    RetrainPredictor,
    CreatePredictor,
    DropPredictor
)
from lightwood import __version__ as lightwood_version

from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.utils import recur_get_conditionals, get_aliased_columns, get_join_input, get_model_name
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.interfaces.model.learn_process import brack_to_mod, rep_recur
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import mark_process
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb import __version__ as mindsdb_version

from .utils import unpack_jsonai_old_args, load_predictor
from .join_utils import get_ts_join_input


class LightwoodHandler(PredictiveHandler):

    name = 'lightwood'

    def __init__(self, name, **kwargs):
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

        self.handler_controller = kwargs.get('handler_controller')
        self.fs_store = kwargs.get('fs_store')
        self.model_controller = kwargs.get('model_controller')
        self.company_id = kwargs.get('company_id')

    # def connect(self, **kwargs) -> Dict[str, int]:
    #     """ Setup storage handler and check lightwood version """  # noqa
    #     self.storage = SqliteStorageHandler(context=self.name, config=kwargs['config'])  # TODO non-KV storage handler?
    #     return self.check_connection()

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

    def get_tables(self) -> Response:
        """ Returns list of model names (that have been succesfully linked with CREATE PREDICTOR) """  # noqa
        # all_models = self.model_controller.get_models()
        # all_models_names = [x['name'] for x in all_models]

        q = "SHOW TABLES;"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result
        # models = self.storage.get('models')
        # return list(models.keys()) if models else []

    def _get_tables_names(self) -> List[str]:
        response = self.get_tables()
        data = response.data_frame.to_dict(orient='records')
        return [x['table_name'] for x in data]

    def describe_table(self, table_name: str) -> Dict:
        """ For getting standard info about a table. e.g. data types """  # noqa
        if table_name not in self.get_tables():
            print("Table not found.")
            return {}
        return self.storage.get('models')[table_name]['jsonai']

    def _run_generate(self, df: pd.DataFrame, problem_definition: ProblemDefinition, predictor_id: int, json_ai_override: dict = None):
        json_ai = lightwood.json_ai_from_problem(df, problem_definition)
        if json_ai_override is None:
            json_ai_override = {}

        json_ai_override = brack_to_mod(json_ai_override)
        json_ai = json_ai.to_dict()
        rep_recur(json_ai, json_ai_override)
        json_ai = JsonAI.from_dict(json_ai)

        code = lightwood.code_from_json_ai(json_ai)

        predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
        predictor_record.json_ai = json_ai.to_dict()
        predictor_record.code = code
        db.session.commit()

    def _run_fit(self, predictor_id: int, df: pd.DataFrame) -> None:
        try:
            predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
            assert predictor_record is not None

            config = Config()

            predictor_record.data = {'training_log': 'training'}
            db.session.commit()
            predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
            predictor.learn(df)

            db.session.refresh(predictor_record)

            fs_name = f'predictor_{predictor_record.company_id}_{predictor_record.id}'
            pickle_path = os.path.join(config['paths']['predictors'], fs_name)
            predictor.save(pickle_path)

            self.fs_store.put(fs_name, fs_name, config['paths']['predictors'])

            predictor_record.data = predictor.model_analysis.to_dict()

            # getting training time for each tried model. it is possible to do
            # after training only
            fit_mixers = list(predictor.runtime_log[x] for x in predictor.runtime_log
                              if isinstance(x, tuple) and x[0] == "fit_mixer")
            submodel_data = predictor_record.data.get("submodel_data", [])
            # add training time to other mixers info
            if submodel_data and fit_mixers and len(submodel_data) == len(fit_mixers):
                for i, tr_time in enumerate(fit_mixers):
                    submodel_data[i]["training_time"] = tr_time
            predictor_record.data["submodel_data"] = submodel_data

            predictor_record.dtype_dict = predictor.dtype_dict
            db.session.commit()
        except Exception as e:
            db.session.refresh(predictor_record)
            predictor_record.data = {'error': f'{traceback.format_exc()}\nMain error: {e}'}
            db.session.commit()
            raise e

    @mark_process(name='learn')
    def _learn(self, statement):
        # TODO do it async
        model_name = statement.name.parts[-1]

        if model_name in self._get_tables_names():
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message="Error: this model already exists!"
            )

        target = statement.targets[0].parts[-1]
        problem_definition_dict = {
            'target': target
        }
        if statement.order_by:
            problem_definition_dict['timeseries_settings'] = {
                'is_timeseries': True,
                'order_by': [str(col) for col in statement.order_by],
                'group_by': [str(col) for col in statement.group_by],
                'window': int(statement.window),
                'horizon': int(statement.horizon),
            }

        json_ai_override = statement.using if statement.using else {}
        unpack_jsonai_old_args(json_ai_override)

        integration_name = str(statement.integration_name)
        handler = self.handler_controller.get_handler(integration_name)
        response = handler.query(statement.query_str)
        if response.type == RESPONSE_TYPE.ERROR:
            return response
        training_data_df = response.data_frame

        integration_meta = self.handler_controller.get(name=integration_name)
        problem_definition = ProblemDefinition.from_dict(problem_definition_dict)

        predictor_record = db.Predictor(
            company_id=self.company_id,
            name=model_name,
            integration_id=integration_meta['id'],
            fetch_data_query=statement.query_str,
            mindsdb_version=mindsdb_version,
            lightwood_version=lightwood_version,
            to_predict=problem_definition.target,
            learn_args=problem_definition.to_dict(),
            data={'name': model_name},
            training_data_columns_count=len(training_data_df.columns),
            training_data_rows_count=len(training_data_df),
            training_start_at=datetime.now()
        )

        db.session.add(predictor_record)
        db.session.commit()

        predictor_id = predictor_record.id

        try:
            self._run_generate(training_data_df, problem_definition, predictor_id, json_ai_override)
            self._run_fit(predictor_id, training_data_df)
        except Exception as e:
            print(traceback.format_exc())
            try:
                exception_type, _exception_object, exception_traceback = sys.exc_info()
                filename = exception_traceback.tb_frame.f_code.co_filename
                line_number = exception_traceback.tb_lineno
                error_message = f'{exception_type.__name__}: {e}, raised at: {filename}#{line_number}'
            except Exception:
                error_message = str(e)

            predictor_record.data = {"error": error_message}
            db.session.commit()

        predictor_record.training_stop_at = datetime.now()
        db.session.commit()

        return Response(RESPONSE_TYPE.OK)

    def native_query(self, query: str) -> Response:
        statement = self.parser(query, dialect=self.dialect)
        config = Config()

        if type(statement) == Show:
            if statement.category.lower() == 'tables':
                all_models = self.model_controller.get_models()
                all_models_names = [[x['name']] for x in all_models]
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    pd.DataFrame(
                        all_models_names,
                        columns=['table_name']
                    )
                )
                return response
            else:
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Cant determine how to show '{statement.category}'"
                )
            return response
        if type(statement) == CreatePredictor:
            return self._learn(statement)
        elif type(statement) == RetrainPredictor:
            model_name = statement.name.parts[-1]

            predictor_record = db.Predictor.query.filter_by(
                company_id=self.company_id, name=model_name
            ).first()
            if predictor_record is None:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Error: model '{model_name}' does not exists!"
                )

            predictor_record.update_status = 'updating'
            db.session.commit()

            handler_meta = self.handler_controller.get(predictor_record.integration_id)
            handler = self.handler_controller.get_handler(handler_meta['name'])
            response = handler.query(predictor_record.fetch_data_query)
            if response.type == RESPONSE_TYPE.ERROR:
                return response

            try:
                problem_definition = predictor_record.learn_args
                problem_definition['target'] = predictor_record.to_predict[0]

                json_ai = lightwood.json_ai_from_problem(response.data_frame, problem_definition)
                predictor_record.json_ai = json_ai.to_dict()
                predictor_record.code = lightwood.code_from_json_ai(json_ai)
                predictor_record.data = {'training_log': 'training'}
                db.session.commit()
                predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
                predictor.learn(response.data_frame)

                fs_name = f'predictor_{predictor_record.company_id}_{predictor_record.id}'
                pickle_path = os.path.join(config['paths']['predictors'], fs_name)
                predictor.save(pickle_path)
                self.fs_store.put(fs_name, fs_name, config['paths']['predictors'])
                predictor_record.data = predictor.model_analysis.to_dict()  # type: ignore
                db.session.commit()

                predictor_record.lightwood_version = lightwood_version
                predictor_record.mindsdb_version = mindsdb_version
                predictor_record.update_status = 'up_to_date'
                db.session.commit()
            except Exception as e:
                predictor_record.update_status = 'update_failed'  # type: ignore
                db.session.commit()
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )

            return Response(RESPONSE_TYPE.OK)
        elif type(statement) == DropPredictor:
            model_name = statement.name.parts[-1]
            all_models = self.model_controller.get_models()
            all_models_names = [x['name'] for x in all_models]
            if model_name not in all_models_names:
                raise Exception(f"Model '{model_name}' does not exists")
            self.model_controller.delete_model(model_name)
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
