import json
import copy
from datetime import datetime

from lightwood.api import dtype
import pandas as pd
import numpy as np
import dfsql

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.postgres.postgres import PostgreSQL
from mindsdb.integrations.mariadb.mariadb import Mariadb
from mindsdb.integrations.mysql.mysql import MySQL
from mindsdb.integrations.mssql.mssql import MSSQL
from mindsdb.utilities.functions import cast_row_types
from mindsdb.utilities.config import Config
from mindsdb.interfaces.database.integrations import get_db_integration
from mindsdb.api.mysql.mysql_proxy.utilities.sql import to_moz_sql_struct


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

    def __init__(self, model_interface, ai_table, data_store, company_id):
        self.config = Config()
        self.company_id = company_id
        self.model_interface = model_interface
        self.ai_table = ai_table
        self.data_store = data_store

    def getTables(self):
        models = self.model_interface.get_models()
        models = [x['name'] for x in models if x['status'] == 'complete']
        models += ['predictors', 'commands']

        ai_tables = self.ai_table.get_ai_tables()
        models += [x['name'] for x in ai_tables]
        return models

    def hasTable(self, table):
        return table in self.getTables()

    def _get_ai_table_columns(self, table_name):
        aitable_record = self.ai_table.get_ai_table(table_name)
        columns = (
            [x['name'] for x in aitable_record.query_fields] + [x['name'] for x in aitable_record.predictor_columns]
        )
        return columns

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
            if dtype_dict[col] in (dtype.integer, dtype.float):
                columns += [f"{col}_min", f"{col}_max"]
            columns += [f"{col}_confidence"]
            columns += [f"{col}_explain"]
        return columns

    def getTableColumns(self, table):
        if table == 'predictors':
            return ['name', 'status', 'accuracy', 'predict', 'select_data_query', 'external_datasource', 'training_options']
        if table == 'commands':
            return ['command']

        columns = []

        ai_tables = self.ai_table.get_ai_table(table)
        if ai_tables is not None:
            columns = self._get_ai_table_columns(table)
        elif table in [x['name'] for x in self.model_interface.get_models()]:
            columns = self._get_model_columns(table)
            columns += ['when_data', 'select_data_query', 'external_datasource']

        return columns

    def _select_predictors(self):
        models = self.model_interface.get_models()
        columns = ['name', 'status', 'accuracy', 'predict', 'select_data_query', 'external_datasource', 'training_options']
        return pd.DataFrame([[
            x['name'],
            x['status'],
            str(x['accuracy']) if x['accuracy'] is not None else None,
            ', '.join(x['predict']) if isinstance(x['predict'], list) else x['predict'],
            '',
            '',  # TODO
            ''  # TODO ?
        ] for x in models], columns=columns)

    def delete_predictor(self, name):
        self.model_interface.delete_model(name)

    def _select_from_ai_table(self, table, columns, where):
        aitable_record = self.ai_table.get_ai_table(table)
        integration = aitable_record.integration_name
        query = aitable_record.integration_query
        predictor_name = aitable_record.predictor_name

        ds_name = self.data_store.get_vacant_name('temp')
        self.data_store.save_datasource(ds_name, integration, {'query': query})
        dso = self.data_store.get_datasource_obj(ds_name, raw=True)
        res = self.model_interface.predict(predictor_name, dso, 'dict')
        self.data_store.delete_datasource(ds_name)

        keys_map = {}
        for f in aitable_record.predictor_columns:
            keys_map[f['value']] = f['name']
        for f in aitable_record.query_fields:
            keys_map[f['name']] = f['name']
        keys = list(keys_map.keys())

        data = []
        for i, el in enumerate(res):
            data.append({keys_map[key]: el[key] for key in keys})

        return data

    def get_predictors(self, mindsdb_sql_query):
        predictors_df = self._select_predictors()
        mindsdb_sql_query.from_table.parts = ['predictors']

        # +++ FIXME https://github.com/mindsdb/dfsql/issues/37 https://github.com/mindsdb/mindsdb_sql/issues/53
        if ' 1 = 0' in str(mindsdb_sql_query):
            q = str(mindsdb_sql_query)
            q = q[:q.lower().find('where')] + ' limit 0'
            result_df = dfsql.sql_query(
                q,
                ds_kwargs={'case_sensitive': False},
                reduce_output=False,
                predictors=predictors_df
            )
        elif 'AND (1 = 1)' in str(mindsdb_sql_query):
            q = str(mindsdb_sql_query)
            q = q.replace('AND (1 = 1)', ' ')
            result_df = dfsql.sql_query(
                q,
                ds_kwargs={'case_sensitive': False},
                reduce_output=False,
                predictors=predictors_df
            )
        else:
            # ---
            try:
                result_df = dfsql.sql_query(
                    str(mindsdb_sql_query),
                    ds_kwargs={'case_sensitive': False},
                    reduce_output=False,
                    predictors=predictors_df
                )
            except Exception:
                # FIXME https://github.com/mindsdb/dfsql/issues/38
                result_df = predictors_df

        # FIXME https://github.com/mindsdb/dfsql/issues/38
        result_df = result_df.where(pd.notnull(result_df), '')

        return result_df.to_dict(orient='records'), list(result_df.columns)

    def select_query(self, query):
        moz_struct = to_moz_sql_struct(query)
        data = self.select(
            table=query.from_table.parts[-1],
            columns=None,
            where=moz_struct.get('where')
        )
        return data

    def select(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None, integration_name=None, integration_type=None, is_timeseries=False):
        ''' NOTE WHERE statements can be just $eq joined with 'and'
        '''
        _mdb_make_predictions = is_timeseries
        if table == 'predictors':
            return self._select_predictors()
        if table == 'commands':
            return []
        if self.ai_table.get_ai_table(table):
            return self._select_from_ai_table(table, columns, where)

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
        if integration_name is not None and 'select_data_query' in where_data:
            select_data_query = where_data['select_data_query']
            del where_data['select_data_query']

            integration_data = get_db_integration(integration_name, self.company_id)
            if integration_type == 'clickhouse':
                ch = Clickhouse(self.config, integration_name, integration_data)
                res = ch._query(select_data_query.strip(' ;\n') + ' FORMAT JSON')
                data = res.json()['data']
            elif integration_type == 'mariadb':
                maria = Mariadb(self.config, integration_name, integration_data)
                data = maria._query(select_data_query)
            elif integration_type == 'mysql':
                mysql = MySQL(self.config, integration_name, integration_data)
                data = mysql._query(select_data_query)
            elif integration_type == 'postgres':
                mysql = PostgreSQL(self.config, integration_name, integration_data)
                data = mysql._query(select_data_query)
            elif integration_type == 'mssql':
                mssql = MSSQL(self.config, integration_name, integration_data)
                data = mssql._query(select_data_query, fetch=True)
            else:
                raise Exception(f'Unknown database type: {integration_type}')

            where_data = data

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

        model = self.model_interface.get_model_data(name=table)
        columns = list(model['dtype_dict'].keys())

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
            group_by = timeseries_settings['group_by']
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

            predict = model['predict']
            data_column = timeseries_settings['order_by'][0]
            nr_predictions = timeseries_settings['nr_predictions']

            for group, data in rows_by_groups.items():
                rows = data['rows']
                explanations = data['explanations']

                if len(rows) == 0:
                    break

                for row in rows:
                    predictions = row[predict]
                    if isinstance(predictions, list) is False:
                        predictions = [predictions]

                    date_values = row[data_column]
                    if isinstance(date_values, list) is False:
                        date_values = [date_values]

                for i in range(len(rows) - 1):
                    rows[i][predict] = rows[i][predict][0]
                    rows[i][data_column] = rows[i][data_column][0]
                    for col in ('predicted_value', 'confidence', 'confidence_lower_bound', 'confidence_upper_bound'):
                        explanations[i][predict][col] = explanations[i][predict][col][0]

                last_row = rows.pop()
                last_explanation = explanations.pop()
                for i in range(nr_predictions):
                    new_row = copy.deepcopy(last_row)
                    new_row[predict] = new_row[predict][i]
                    new_row[data_column] = new_row[data_column][i]
                    rows.append(new_row)

                    new_explanation = copy.deepcopy(last_explanation)
                    for col in ('predicted_value', 'confidence', 'confidence_lower_bound', 'confidence_upper_bound'):
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

            if model['dtypes'][data_column] == dtype.date:
                for row in pred_dicts:
                    if isinstance(row[data_column], (int, float)):
                        row[data_column] = str(datetime.fromtimestamp(row[data_column]).date())
            elif model['dtypes'][data_column] == dtype.datetime:
                for row in pred_dicts:
                    if isinstance(row[data_column], (int, float)):
                        row[data_column] = str(datetime.fromtimestamp(row[data_column]))

        keys = [x for x in pred_dicts[0] if x in columns]
        min_max_keys = []
        for col in predicted_columns:
            if model['dtype_dict'][col] in (dtype.integer, dtype.float):
                min_max_keys.append(col)

        data = []
        explains = []
        keys_to_save = [*keys, '__mindsdb_row_id', 'external_datasource', 'select_data_query', 'when_data']
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
                row[key + '_min'] = explanation[key]['confidence_lower_bound']
                row[key + '_max'] = explanation[key]['confidence_upper_bound']

        return data
