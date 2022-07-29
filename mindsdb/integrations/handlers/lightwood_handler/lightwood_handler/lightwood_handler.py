import os
import sys
import json
import traceback
from datetime import datetime
from typing import Dict, List, Optional
import copy

import sqlalchemy
import pandas as pd
import lightwood
from lightwood.api.types import JsonAI
from lightwood.api.high_level import json_ai_from_problem, predictor_from_code, code_from_json_ai, ProblemDefinition
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Join, BinaryOperation, Identifier, Constant, Select, OrderBy, Show
from mindsdb_sql.parser.dialects.mindsdb import (
    RetrainPredictor,
    CreatePredictor,
    DropPredictor
)
from lightwood import __version__ as lightwood_version
from lightwood.api import dtype
import numpy as np

from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.utils import recur_get_conditionals, get_aliased_columns, get_join_input, get_model_name
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
from mindsdb.utilities.functions import cast_row_types

from .utils import unpack_jsonai_old_args, load_predictor
from .join_utils import get_ts_join_input


def get_where_data(where):
    result = {}
    if type(where) != BinaryOperation:
        raise Exception("Wrong 'where' statement")
    if where.op == '=':
        if type(where.args[0]) != Identifier or type(where.args[1]) != Constant:
            raise Exception("Wrong 'where' statement")
        result[where.args[0].parts[-1]] = where.args[1].value
    elif where.op == 'and':
        result.update(get_where_data(where.args[0]))
        result.update(get_where_data(where.args[1]))
    else:
        raise Exception("Wrong 'where' statement")
    return result


class NumpyJSONEncoder(json.JSONEncoder):
    """
    Use this encoder to avoid
    "TypeError: Object of type float32 is not JSON serializable"

    Example:
    x = np.float32(5)
    json.dumps(x, cls=NumpyJSONEncoder)
    """
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.float, np.float32, np.float64)):
            return float(obj)
        else:
            return super().default(obj)


class LightwoodHandler(PredictiveHandler):

    name = 'lightwood'

    def __init__(self, name, **kwargs):
        """ Lightwood AutoML integration """  # noqa
        super().__init__(name)
        self.config = Config()
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
        q = "SHOW TABLES;"
        result = self.native_query(q)
        result.data_frame = result.data_frame.rename(
            columns={result.data_frame.columns[0]: 'table_name'}
        )
        return result

    def _get_tables_names(self) -> List[str]:
        response = self.get_tables()
        data = response.data_frame.to_dict(orient='records')
        return [x['table_name'] for x in data]

    def get_columns(self, table_name: str) ->  Response:
        """ For getting standard info about a table. e.g. data types """  # noqa
        predictor_record = db.Predictor.query.filter_by(
            company_id=self.company_id, name=table_name
        ).first()
        if predictor_record is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{table_name}' does not exists!"
            )

        data = []
        if predictor_record.dtype_dict is not None:
            for key, value in predictor_record.dtype_dict.items():
                data.appen(key, value)
        result = Response(
            RESPONSE_TYPE.TABLE,
            pd.DataFrame(
                data,
                columns=['COLUMN_NAME', 'DATA_TYPE']
            )
        )
        return result

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

            predictor_record.data = {'training_log': 'training'}
            db.session.commit()
            predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
            predictor.learn(df)

            db.session.refresh(predictor_record)

            fs_name = f'predictor_{predictor_record.company_id}_{predictor_record.id}'
            pickle_path = os.path.join(self.config['paths']['predictors'], fs_name)
            predictor.save(pickle_path)

            self.fs_store.put(fs_name, fs_name, self.config['paths']['predictors'])

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
            order_by = statement.order_by[0].field.parts[-1]
            group_by = [x.parts[-1] for x in statement.group_by]

            problem_definition_dict['timeseries_settings'] = {
                'is_timeseries': True,
                'order_by': order_by,
                'group_by': group_by,
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
        query_ast = self.parser(query, dialect=self.dialect)
        return self.query(query_ast)

    def query(self, query: ASTNode) -> Response:
        statement = query
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
        elif type(statement) == Select:
            model_name = statement.from_table.parts[-1]
            where_data = get_where_data(statement.where)
            predictions = self.predict(model_name, where_data)
            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(predictions)
            )
        else:
            raise Exception(f"Query type {type(statement)} not supported")

    def predict(self, model_name: str, data: list) -> pd.DataFrame:
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        predictor_record = db.Predictor.query.filter_by(
            company_id=self.company_id, name=model_name
        ).first()
        if predictor_record is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{model_name}' does not exists!"
            )

        # TODO Add predictor cache!!!

        fs_name = f'predictor_{self.company_id}_{predictor_record.id}'
        self.fs_store.get(fs_name, fs_name, self.config['paths']['predictors'])
        predictor = lightwood.predictor_from_state(
            os.path.join(self.config['paths']['predictors'], fs_name),
            predictor_record.code
        )
        predictions = predictor.predict(df)
        predictions = predictions.to_dict(orient='records')

        # region format result
        target = predictor_record.to_predict[0]
        explain_arr = []
        pred_dicts = []
        for i, row in enumerate(predictions):
            obj = {
                target: {
                    'predicted_value': row['prediction'],
                    'confidence': row.get('confidence', None),
                    'anomaly': row.get('anomaly', None),
                    'truth': row.get('truth', None)
                }
            }
            if 'lower' in row:
                obj[target]['confidence_lower_bound'] = row.get('lower', None)
                obj[target]['confidence_upper_bound'] = row.get('upper', None)

            explain_arr.append(obj)

            td = {'predicted_value': row['prediction']}
            for col in df.columns:
                if col in row:
                    td[col] = row[col]
                elif f'order_{col}' in row:
                    td[col] = row[f'order_{col}']
                elif f'group_{col}' in row:
                    td[col] = row[f'group_{col}']
                else:
                    orginal_index = row.get('original_index')
                    if orginal_index is None:
                        orginal_index = i
                    td[col] = df.iloc[orginal_index][col]
            pred_dicts.append({target: td})

        new_pred_dicts = []
        for row in pred_dicts:
            new_row = {}
            for key in row:
                new_row.update(row[key])
                new_row[key] = new_row['predicted_value']
            del new_row['predicted_value']
            new_pred_dicts.append(new_row)
        pred_dicts = new_pred_dicts

        columns = list(predictor_record.dtype_dict.keys())
        predicted_columns = predictor_record.to_predict
        if not isinstance(predicted_columns, list):
            predicted_columns = [predicted_columns]
        # endregion

        original_target_values = {}
        for col in predicted_columns:
            df = df.reset_index()
            original_target_values[col + '_original'] = []
            for _index, row in df.iterrows():
                original_target_values[col + '_original'].append(row.get(col))

        # region transform ts predictions
        timeseries_settings = predictor_record.learn_args['timeseries_settings']

        if timeseries_settings['is_timeseries'] is True:
            __no_forecast_offset = set([row.get('__mdb_forecast_offset', None) for row in pred_dicts]) == {None}

            predict = predictor_record.to_predict[0]
            group_by = timeseries_settings['group_by'] or []
            order_by_column = timeseries_settings['order_by']
            if isinstance(order_by_column, list):
                order_by_column = order_by_column[0]
            horizon = timeseries_settings['horizon']

            groups = set()
            for row in pred_dicts:
                groups.add(
                    tuple([row[x] for x in group_by])
                )

            # split rows by groups
            rows_by_groups = {}
            for group in groups:
                rows_by_groups[group] = {
                    'rows': [],
                    'explanations': []
                }
                for row_index, row in enumerate(pred_dicts):
                    is_wrong_group = False
                    for i, group_by_key in enumerate(group_by):
                        if row[group_by_key] != group[i]:
                            is_wrong_group = True
                            break
                    if not is_wrong_group:
                        rows_by_groups[group]['rows'].append(row)
                        rows_by_groups[group]['explanations'].append(explain_arr[row_index])

            for group, data in rows_by_groups.items():
                rows = data['rows']
                explanations = data['explanations']

                if len(rows) == 0:
                    break

                for row in rows:
                    predictions = row[predict]
                    if isinstance(predictions, list) is False:
                        predictions = [predictions]

                    date_values = row[order_by_column]
                    if isinstance(date_values, list) is False:
                        date_values = [date_values]

                for i in range(len(rows) - 1):
                    if horizon > 1:
                        rows[i][predict] = rows[i][predict][0]
                        if isinstance(rows[i][order_by_column], list):
                            rows[i][order_by_column] = rows[i][order_by_column][0]
                    for col in ('predicted_value', 'confidence', 'confidence_lower_bound', 'confidence_upper_bound'):
                        if horizon > 1:
                            explanations[i][predict][col] = explanations[i][predict][col][0]

                last_row = rows.pop()
                last_explanation = explanations.pop()
                for i in range(horizon):
                    new_row = copy.deepcopy(last_row)
                    if horizon > 1:
                        new_row[predict] = new_row[predict][i]
                        if isinstance(new_row[order_by_column], list):
                            new_row[order_by_column] = new_row[order_by_column][i]
                    if '__mindsdb_row_id' in new_row and (i > 0 or __no_forecast_offset):
                        new_row['__mindsdb_row_id'] = None
                    rows.append(new_row)

                    new_explanation = copy.deepcopy(last_explanation)
                    for col in ('predicted_value', 'confidence', 'confidence_lower_bound', 'confidence_upper_bound'):
                        if horizon > 1:
                            new_explanation[predict][col] = new_explanation[predict][col][i]
                    if i != 0:
                        new_explanation[predict]['anomaly'] = None
                        new_explanation[predict]['truth'] = None
                    explanations.append(new_explanation)

            pred_dicts = []
            explanations = []
            for group, data in rows_by_groups.items():
                pred_dicts.extend(data['rows'])
                explanations.extend(data['explanations'])

            original_target_values[f'{predict}_original'] = []
            for i in range(len(pred_dicts)):
                original_target_values[f'{predict}_original'].append(explanations[i][predict].get('truth', None))

            if predictor_record.dtype_dict[order_by_column] == dtype.date:
                for row in pred_dicts:
                    if isinstance(row[order_by_column], (int, float)):
                        row[order_by_column] = str(datetime.fromtimestamp(row[order_by_column]).date())
            elif predictor_record.dtype_dict[order_by_column] == dtype.datetime:
                for row in pred_dicts:
                    if isinstance(row[order_by_column], (int, float)):
                        row[order_by_column] = str(datetime.fromtimestamp(row[order_by_column]))

            explain_arr = explanations
        # endregion

        keys = [x for x in pred_dicts[0] if x in columns]
        min_max_keys = []
        for col in predicted_columns:
            if predictor_record.dtype_dict[col] in (dtype.integer, dtype.float, dtype.num_tsarray):
                min_max_keys.append(col)

        data = []
        explains = []
        keys_to_save = [*keys, '__mindsdb_row_id', 'select_data_query', 'when_data']
        for i, el in enumerate(pred_dicts):
            data.append({key: el.get(key) for key in keys_to_save})
            explains.append(explain_arr[i])

        for i, row in enumerate(data):
            cast_row_types(row, predictor_record.dtype_dict)

            for k in original_target_values:
                try:
                    row[k] = original_target_values[k][i]
                except Exception:
                    row[k] = None

            for column_name in columns:
                if column_name not in row:
                    row[column_name] = None

            explanation = explains[i]
            for key in predicted_columns:
                row[key + '_confidence'] = explanation[key]['confidence']
                row[key + '_explain'] = json.dumps(explanation[key], cls=NumpyJSONEncoder, ensure_ascii=False)
                if 'anomaly' in explanation[key]:
                    row[key + '_anomaly'] = explanation[key]['anomaly']
            for key in min_max_keys:
                if 'confidence_lower_bound' in explanation[key]:
                    row[key + '_min'] = explanation[key]['confidence_lower_bound']
                if 'confidence_upper_bound' in explanation[key]:
                    row[key + '_max'] = explanation[key]['confidence_upper_bound']
        return data

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
            except Exception:
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
