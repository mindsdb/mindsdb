import sys
import json
import copy
from typing import Optional, Dict
from datetime import datetime

import pandas as pd
from type_infer.dtype import dtype
import lightwood
import numpy as np

import mindsdb.interfaces.storage.db as db

from mindsdb.utilities.functions import cast_row_types
# from mindsdb.utilities.hooks import after_predict as after_predict_hook
from mindsdb.interfaces.model.functions import get_model_record
from mindsdb.interfaces.storage.json import get_json_storage
from mindsdb.integrations.libs.base import BaseMLEngine

from .functions import run_learn, run_adjust

IS_PY36 = sys.version_info[1] <= 6


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


class LightwoodHandler(BaseMLEngine):
    name = 'lightwood'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'df' not in kwargs:
            return
        df = kwargs['df']
        columns = [x.lower() for x in df.columns]
        if target.lower() not in columns:
            raise Exception(f"There is no column '{target}' in dataframe")

        if 'timeseries_settings' in args and args['timeseries_settings'].get('is_timeseries') is True:
            tss = args['timeseries_settings']
            if 'order_by' in tss and tss['order_by'].lower() not in columns:
                raise Exception(f"There is no column '{tss['order_by']}' in dataframe")
            if isinstance(tss.get('group_by'), list):
                for column in tss['group_by']:
                    if column.lower() not in columns:
                        raise Exception(f"There is no column '{column}' in dataframe")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args['target'] = target
        run_learn(
            df,
            args,   # Problem definition and JsonAI override
            self.model_storage
        )

    def update(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        run_adjust(
            df,
            args,
            self.model_storage
        )

    def predict(self, df, args=None):
        pred_format = args['pred_format']
        predictor_code = args['code']
        learn_args = args['learn_args']
        pred_args = args.get('predict_params', {})
        self.model_storage.fileStorage.pull()

        predictor = lightwood.predictor_from_state(
            self.model_storage.fileStorage.folder_path / self.model_storage.fileStorage.folder_name,
            predictor_code
        )
        dtype_dict = predictor.dtype_dict

        predictions = predictor.predict(df, args=pred_args)
        predictions = predictions.to_dict(orient='records')

        # TODO!!!
        # after_predict_hook(
        #     company_id=self.company_id,
        #     predictor_id=predictor_record.id,
        #     rows_in_count=df.shape[0],
        #     columns_in_count=df.shape[1],
        #     rows_out_count=len(predictions)
        # )

        # region format result
        target = args['target']
        explain_arr = []
        pred_dicts = []
        for i, row in enumerate(predictions):
            values = {
                'predicted_value': row['prediction'],
                'confidence': row.get('confidence', None),
                'anomaly': row.get('anomaly', None),
                'truth': row.get('truth', None)
            }

            if predictor.supports_proba:
                for cls in predictor.statistical_analysis.train_observed_classes:
                    if row.get(f'__mdb_proba_{cls}', False):
                        values[f'probability_class_{cls}'] = round(row[f'__mdb_proba_{cls}'], 4)

            for block in predictor.analysis_blocks:
                if type(block).__name__ == 'ShapleyValues':
                    cols = block.columns
                    values['shap_base_response'] = round(row['shap_base_response'], 4)
                    values['shap_final_response'] = round(row['shap_final_response'], 4)
                    for col in cols:
                        values[f'shap_contribution_{col}'] = round(row[f'shap_contribution_{col}'], 4)

            if 'lower' in row:
                values['confidence_lower_bound'] = row.get('lower', None)
                values['confidence_upper_bound'] = row.get('upper', None)

            obj = {target: values}
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

        columns = list(dtype_dict.keys())
        predicted_columns = target
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
        timeseries_settings = learn_args.get('timeseries_settings', {'is_timeseries': False})

        if timeseries_settings['is_timeseries'] is True:
            # offset forecast if have __mdb_forecast_offset > 0
            forecast_offset = any([
                row.get('__mdb_forecast_offset') is not None and row['__mdb_forecast_offset'] > 0
                for row in pred_dicts
            ])

            group_by = timeseries_settings.get('group_by', [])
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
                    predictions = row[target]
                    if isinstance(predictions, list) is False:
                        predictions = [predictions]

                    date_values = row[order_by_column]
                    if isinstance(date_values, list) is False:
                        date_values = [date_values]

                for i in range(len(rows) - 1):
                    if horizon > 1:
                        rows[i][target] = rows[i][target][0]
                        if isinstance(rows[i][order_by_column], list):
                            rows[i][order_by_column] = rows[i][order_by_column][0]
                    for col in ('predicted_value', 'confidence', 'confidence_lower_bound', 'confidence_upper_bound'):
                        if horizon > 1 and col in explanations[i][target]:
                            explanations[i][target][col] = explanations[i][target][col][0]

                last_row = rows.pop()
                last_explanation = explanations.pop()
                for i in range(horizon):
                    new_row = copy.deepcopy(last_row)
                    if horizon > 1:
                        new_row[target] = new_row[target][i]
                        if isinstance(new_row[order_by_column], list):
                            new_row[order_by_column] = new_row[order_by_column][i]
                    if '__mindsdb_row_id' in new_row and (i > 0 or forecast_offset):
                        new_row['__mindsdb_row_id'] = None
                    rows.append(new_row)

                    new_explanation = copy.deepcopy(last_explanation)
                    for col in ('predicted_value', 'confidence', 'confidence_lower_bound', 'confidence_upper_bound'):
                        if horizon > 1 and col in new_explanation[target]:
                            new_explanation[target][col] = new_explanation[target][col][i]
                    if i != 0:
                        new_explanation[target]['anomaly'] = None
                        new_explanation[target]['truth'] = None
                    explanations.append(new_explanation)

            pred_dicts = []
            explanations = []
            for group, data in rows_by_groups.items():
                pred_dicts.extend(data['rows'])
                explanations.extend(data['explanations'])

            original_target_values[f'{target}_original'] = []
            for i in range(len(pred_dicts)):
                original_target_values[f'{target}_original'].append(explanations[i][target].get('truth', None))

            if dtype_dict[order_by_column] == dtype.date:
                for row in pred_dicts:
                    if isinstance(row[order_by_column], (int, float)):
                        row[order_by_column] = datetime.fromtimestamp(row[order_by_column]).date()
            elif dtype_dict[order_by_column] == dtype.datetime:
                for row in pred_dicts:
                    if isinstance(row[order_by_column], (int, float)):
                        row[order_by_column] = datetime.fromtimestamp(row[order_by_column])

            explain_arr = explanations
        # endregion

        if pred_format == 'explain':
            return explain_arr

        keys = [x for x in pred_dicts[0] if x in columns]
        min_max_keys = []
        for col in predicted_columns:
            if dtype_dict[col] in (dtype.integer, dtype.float, dtype.num_tsarray):
                min_max_keys.append(col)

        data = []
        explains = []
        keys_to_save = [*keys, '__mindsdb_row_id', 'select_data_query', 'when_data']
        for i, el in enumerate(pred_dicts):
            data.append({key: el.get(key) for key in keys_to_save})
            explains.append(explain_arr[i])

        for i, row in enumerate(data):
            cast_row_types(row, dtype_dict)

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

        return pd.DataFrame(data)

    def edit_json_ai(self, name: str, json_ai: dict):
        predictor_record = get_model_record(name=name, ml_handler_name='lightwood')
        assert predictor_record is not None

        json_ai = lightwood.JsonAI.from_dict(json_ai)
        predictor_record.code = lightwood.code_from_json_ai(json_ai)
        db.session.commit()

        json_storage = get_json_storage(
            resource_id=predictor_record.id
        )
        json_storage.set('json_ai', json_ai.to_dict())

    def code_from_json_ai(self, json_ai: dict):
        json_ai = lightwood.JsonAI.from_dict(json_ai)
        code = lightwood.code_from_json_ai(json_ai)
        return code

    def edit_code(self, name: str, code: str):
        """Edit an existing predictor's code"""
        if self.config.get('cloud', False):
            raise Exception('Code editing prohibited on cloud')

        predictor_record = get_model_record(name=name, ml_handler_name='lightwood')
        assert predictor_record is not None

        lightwood.predictor_from_code(code)
        predictor_record.code = code
        db.session.commit()

        json_storage = get_json_storage(
            resource_id=predictor_record.id
        )
        json_storage.delete('json_ai')

    def _get_features_info(self):
        ai_info = self.model_storage.json_get('json_ai')
        if ai_info == {}:
            raise Exception("predictor doesn't contain enough data to generate 'feature' attribute.")
        data = []
        dtype_dict = ai_info["dtype_dict"]
        for column in dtype_dict:
            c_data = []
            c_data.append(column)
            c_data.append(dtype_dict[column])
            c_data.append(ai_info["encoders"][column]["module"])
            if ai_info["encoders"][column]["args"].get("is_target", "False") == "True":
                c_data.append("target")
            else:
                c_data.append("feature")
            data.append(c_data)

        return pd.DataFrame(data, columns=['column', 'type', 'encoder', 'role'])

    def _get_model_info(self):
        json_ai = self.model_storage.json_get('json_ai')
        model_info = self.model_storage.get_info()
        model_data = model_info['data']

        accuracy_functions = json_ai.get('accuracy_functions')
        if accuracy_functions:
            accuracy_functions = str(accuracy_functions)

        models_data = model_data.get("submodel_data", [])
        if models_data == []:
            raise Exception("predictor doesn't contain enough data to generate 'model' attribute")
        data = []

        for model in models_data:
            m_data = []
            m_data.append(model["name"])
            m_data.append(model["accuracy"])
            m_data.append(model.get("training_time", "unknown"))
            m_data.append(1 if model["is_best"] else 0)
            m_data.append(accuracy_functions)
            data.append(m_data)

        return pd.DataFrame(data, columns=['name', 'performance', 'training_time', 'selected', 'accuracy_functions'])

    def _get_ensemble_data(self):
        ai_info = self.model_storage.json_get('json_ai')
        if ai_info == {}:
            raise Exception("predictor doesn't contain enough data to generate 'ensamble' attribute. Please wait until predictor is complete.")
        ai_info_str = json.dumps(ai_info, indent=2)

        return pd.DataFrame([[ai_info_str]], columns=['ensemble'])

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        if attribute is None:

            model_description = {}

            model_info = self.model_storage.get_info()
            model_data = model_info['data']
            to_predict = model_info['to_predict'][0]
            json_ai = self.model_storage.json_get('json_ai')

            if model_data.get('accuracies', None) is not None:
                if len(model_data['accuracies']) > 0:
                    model_data['accuracy'] = float(np.mean(list(model_data['accuracies'].values())))

            model_columns = self.model_storage.columns_get()

            model_description['accuracies'] = model_data['accuracies']
            model_description['column_importances'] = model_data['column_importances']
            model_description['outputs'] = [to_predict]
            model_description['inputs'] = [col for col in model_columns if col not in model_description['outputs']]
            model_description['model'] = ' --> '.join(str(k) for k in json_ai)

            return pd.DataFrame([model_description])

        else:
            if attribute == "features":
                return self._get_features_info()

            elif attribute == "model":
                return self._get_model_info()

            elif attribute == "ensemble":
                return self._get_ensemble_data()

            else:
                raise Exception("DESCRIBE '%s' predictor attribute is not supported yet" % attribute)

