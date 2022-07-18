import json
import copy
from datetime import datetime

from lightwood.api import dtype
import pandas as pd
import numpy as np

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.utilities.functions import get_column_in_case
from mindsdb.utilities.functions import cast_row_types
from mindsdb.utilities.config import Config
from mindsdb.api.mysql.mysql_proxy.utilities import SqlApiException
from mindsdb.api.mysql.mysql_proxy.datahub.classes.tables_row import TablesRow


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


class MindsDBDataNode(DataNode):
    type = 'mindsdb'

    def __init__(self, model_interface, integration_controller):
        self.config = Config()
        self.model_interface = model_interface
        self.integration_controller = integration_controller

    def get_tables(self):
        models = self.model_interface.get_models()
        tables = []
        for model in models:
            tables.append(TablesRow(TABLE_NAME=model['name']))
        tables.append(TablesRow(TABLE_NAME='predictors'))
        tables.append(TablesRow(TABLE_NAME='databases'))

        return tables

    def has_table(self, table):
        names = [table.TABLE_NAME for table in self.get_tables()]
        return table in names

    def _get_model_columns(self, table_name):
        model = self.model_interface.get_model_data(name=table_name)
        dtype_dict = model.get('dtype_dict')
        if isinstance(dtype_dict, dict) is False:
            return []
        columns = []
        columns += list(dtype_dict.keys())
        predict = model['predict']
        if not isinstance(predict, list):
            predict = [predict]
        columns += [f'{x}_original' for x in predict]
        for col in predict:
            if dtype_dict.get(col) in (dtype.integer, dtype.float):
                columns += [f"{col}_min", f"{col}_max"]
            columns += [f"{col}_confidence"]
            columns += [f"{col}_explain"]
        return columns

    def get_table_columns(self, table):
        if table == 'predictors':
            return ['name', 'status', 'accuracy', 'predict', 'update_status',
                    'mindsdb_version', 'error', 'select_data_query',
                    'training_options']
        if table in ('datasources', 'databases'):
            return ['name', 'database_type', 'host', 'port', 'user']

        columns = []

        if table in [x['name'] for x in self.model_interface.get_models()]:
            columns = self._get_model_columns(table)
            columns += ['when_data', 'select_data_query']

        return columns

    def _select_predictors(self):
        models = self.model_interface.get_models()
        columns = ['name', 'status', 'accuracy', 'predict', 'update_status',
                   'mindsdb_version', 'error', 'select_data_query',
                   'training_options']
        return pd.DataFrame([[
            x['name'],
            x['status'],
            str(x['accuracy']) if x['accuracy'] is not None else None,
            ', '.join(x['predict']) if isinstance(x['predict'], list) else x['predict'],
            x['update'],
            x['mindsdb_version'],
            x['error'],
            '',
            ''   # TODO
        ] for x in models], columns=columns)

    def _select_integrations(self):
        integrations = self.integration_controller.get_all()
        result = []
        for ds_name, ds_meta in integrations.items():
            connection_data = ds_meta.get('connection_data', {})
            result.append([
                ds_name, ds_meta.get('engine'), connection_data.get('host'), connection_data.get('port'), connection_data.get('user')
            ])
        return pd.DataFrame(
            result,
            columns=['name', 'database_type', 'host', 'port', 'user']
        )

    def delete_predictor(self, name):
        self.model_interface.delete_model(name)

    def get_predictors(self, mindsdb_sql_query):
        predictors_df = self._select_predictors()

        try:
            result_df = query_df(predictors_df, mindsdb_sql_query)
        except Exception as e:
            print(f'Exception! {e}')
            return [], []

        # FIXME https://github.com/mindsdb/dfsql/issues/38
        # TODO remove it whem wll be sure query_df do properly casting
        # result_df = result_df.where(pd.notnull(result_df), '')

        return result_df.to_dict(orient='records'), list(result_df.columns)

    def get_integrations(self, mindsdb_sql_query):
        datasources_df = self._select_integrations()
        try:
            result_df = query_df(datasources_df, mindsdb_sql_query)
        except Exception as e:
            print(f'Exception! {e}')
            return [], []
        return result_df.to_dict(orient='records'), list(result_df.columns)

    def query(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None, integration_name=None, integration_type=None):
        ''' NOTE WHERE statements can be just $eq joined with 'and'
        '''
        if table == 'predictors':
            return self._select_predictors()
        if table == 'datasources':
            return self._select_datasources()

        original_when_data = None
        if 'when_data' in where_data:
            if len(where_data) > 1:
                raise ValueError("Should not be used any other keys in 'where', if 'when_data' used")
            try:
                original_when_data = where_data['when_data']
                where_data = json.loads(where_data['when_data'])
                if isinstance(where_data, list) is False:
                    where_data = [where_data]
            except Exception:
                raise ValueError(f'''Error while parse 'when_data'="{where_data}"''')

        select_data_query = None

        new_where = {}
        if where_data is None:
            for key, value in where_data.items():
                if isinstance(value, dict) is False or len(value.keys()) != 1 or list(value.keys())[0] != '$eq':
                    # TODO value should be just string or number
                    raise Exception()
                new_where[key] = value['$eq']

            if len(new_where) == 0:
                return []

            where_data = [new_where]

        if isinstance(where_data, dict):
            where_data = [where_data]

        models_sql_meta = self.get_tables()
        if table not in [x.TABLE_NAME for x in models_sql_meta]:
            raise SqlApiException(f"Predictor '{table}' does not exists'")

        model = self.model_interface.get_model_data(name=table)
        if model.get('status') != 'complete':
            raise SqlApiException(
                f"Trying to use predictor '{table}', but the predictor has status '{model.get('status')}'. " +
                "The predictor must has status 'complete' in order to use it."
            )

        columns = list(model['dtype_dict'].keys())

        # cast where_data column to case of original predicto column
        if len(where_data) > 0:
            row = where_data[0]
            col_name_map = {}
            for i, col_name in enumerate(row):
                if col_name in ('__mindsdb_row_id', '__mdb_forecast_offset'):
                    continue
                new_col_name = get_column_in_case(columns, col_name)
                if new_col_name is not None and col_name != new_col_name:
                    col_name_map[col_name] = new_col_name
            if len(col_name_map) > 0:
                for row in where_data:
                    for old_col_name, new_col_name in col_name_map.items():
                        row[new_col_name] = row[old_col_name]
                        del row[old_col_name]

        predicted_columns = model['predict']
        if not isinstance(predicted_columns, list):
            predicted_columns = [predicted_columns]

        original_target_values = {}
        for col in predicted_columns:
            if where_data is not None:
                if col in where_data:
                    original_target_values[col + '_original'] = list(where_data[col])
                else:
                    original_target_values[col + '_original'] = [None] * len(where_data)
            else:
                original_target_values[col + '_original'] = [None]

        pred_dicts, explanations = self.model_interface.predict(table, where_data, 'dict&explain')

        # transform predictions to more convenient view
        new_pred_dicts = []
        for row in pred_dicts:
            new_row = {}
            for key in row:
                new_row.update(row[key])
                new_row[key] = new_row['predicted_value']
            del new_row['predicted_value']
            new_pred_dicts.append(new_row)
        pred_dicts = new_pred_dicts

        timeseries_settings = model['problem_definition']['timeseries_settings']

        if timeseries_settings['is_timeseries'] is True:
            __no_forecast_offset = set([row.get('__mdb_forecast_offset', None) for row in where_data]) == {None}

            predict = model['predict']
            group_by = timeseries_settings['group_by'] or []
            order_by_column = timeseries_settings['order_by'][0]
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
                        rows_by_groups[group]['explanations'].append(explanations[row_index])

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

            if model['dtypes'][order_by_column] == dtype.date:
                for row in pred_dicts:
                    if isinstance(row[order_by_column], (int, float)):
                        row[order_by_column] = str(datetime.fromtimestamp(row[order_by_column]).date())
            elif model['dtypes'][order_by_column] == dtype.datetime:
                for row in pred_dicts:
                    if isinstance(row[order_by_column], (int, float)):
                        row[order_by_column] = str(datetime.fromtimestamp(row[order_by_column]))

        keys = [x for x in pred_dicts[0] if x in columns]
        min_max_keys = []
        for col in predicted_columns:
            if model['dtype_dict'][col] in (dtype.integer, dtype.float, dtype.num_tsarray):
                min_max_keys.append(col)

        data = []
        explains = []
        keys_to_save = [*keys, '__mindsdb_row_id', 'select_data_query', 'when_data']
        for i, el in enumerate(pred_dicts):
            data.append({key: el.get(key) for key in keys_to_save})
            explains.append(explanations[i])

        for i, row in enumerate(data):
            cast_row_types(row, model['dtype_dict'])

            row['select_data_query'] = select_data_query
            row['when_data'] = original_when_data

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
