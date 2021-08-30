import json
import numpy as np
from datetime import datetime

from lightwood.api.dtype import dtype

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
        columns = []
        columns += list(model['dtype_dict'].keys())
        predict = model['predict']
        if not isinstance(predict, list):
            predict = [predict]
        columns += [f'{x}_original' for x in predict]
        for col in predict:
            if model['dtype_dict'][col] in (dtype.integer, dtype.float):
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
        return [{
            'name': x['name'],
            'status': x['status'],
            'accuracy': str(x['accuracy']) if x['accuracy'] is not None else None,
            'predict': ', '.join(x['predict']) if isinstance(x['predict'], list) else x['predict'],
            'select_data_query': '',
            'external_datasource': '',  # TODO
            'training_options': ''  # TODO ?
        } for x in models]

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

    def select_query(self, query):
        from mindsdb.api.mysql.mysql_proxy.utilities.sql import to_moz_sql_struct
        moz_struct = to_moz_sql_struct(query)
        data = self.select(
            table=query.from_table.parts[-1],
            columns=None,
            where=moz_struct.get('where')
        )
        return data

    def select(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None, is_timeseries=False):
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
        if 'when_data' in where:
            if len(where) > 1:
                raise ValueError("Should not be used any other keys in 'where', if 'when_data' used")
            try:
                original_when_data = where['when_data']['$eq']
                where_data = json.loads(where['when_data']['$eq'])
                if isinstance(where_data, list) is False:
                    where_data = [where_data]
            except Exception:
                raise ValueError(f'''Error while parse 'when_data'="{where_data}"''')
        external_datasource = None
        if 'external_datasource' in where:
            external_datasource = where['external_datasource']['$eq']
            del where['external_datasource']

        select_data_query = None
        if came_from is not None and 'select_data_query' in where:
            select_data_query = where['select_data_query']['$eq']
            del where['select_data_query']

            integration_data = get_db_integration(came_from, self.company_id)
            dbtype = integration_data['type']
            if dbtype == 'clickhouse':
                ch = Clickhouse(self.config, came_from, integration_data)
                res = ch._query(select_data_query.strip(' ;\n') + ' FORMAT JSON')
                data = res.json()['data']
            elif dbtype == 'mariadb':
                maria = Mariadb(self.config, came_from, integration_data)
                data = maria._query(select_data_query)
            elif dbtype == 'mysql':
                mysql = MySQL(self.config, came_from, integration_data)
                data = mysql._query(select_data_query)
            elif dbtype == 'postgres':
                mysql = PostgreSQL(self.config, came_from, integration_data)
                data = mysql._query(select_data_query)
            elif dbtype == 'mssql':
                mssql = MSSQL(self.config, came_from, integration_data)
                data = mssql._query(select_data_query, fetch=True)
            else:
                raise Exception(f'Unknown database type: {dbtype}')

            if where_data is None:
                where_data = data
            else:
                where_data += data

        new_where = {}
        if where_data is None:
            for key, value in where.items():
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

        if not model['problem_definition']['timeseries_settings']['is_timeseries']:
            # Fix since for some databases we *MUST* return the same value for the columns originally specified in the `WHERE`
            if isinstance(where_data, list):
                data = []
                for row in pred_dicts:
                    new_row = {}
                    for key in row:
                        new_row.update(row[key])
                        predicted_value = new_row['predicted_value']
                        del new_row['predicted_value']
                        new_row[key] = predicted_value
                    data.append(new_row)
                pred_dicts = data

            if isinstance(where_data, dict):
                for col in where_data:
                    if col not in predicted_columns:
                        pred_dicts[0][col] = where_data[col]

        else:
            predict = model['predict']
            data_column = model['problem_definition']['timeseries_settings']['order_by'][0]
            nr_predictions = model['problem_definition']['timeseries_settings']['nr_predictions']
            new_pred_dicts = []
            if _mdb_make_predictions is False:
                pred_dict = pred_dicts[0]
                predictions = pred_dict[predict]['predicted_value']
                if isinstance(predictions, list) is False:
                    predictions = [predictions]
                data_values = pred_dict[predict][data_column]
                if isinstance(data_values, list) is False:
                    data_values = [data_values]
                for i in range(nr_predictions):
                    nd = {}
                    nd.update(pred_dict[predict])
                    new_pred_dicts.append(nd)
                    nd[predict] = predictions[i]
                    if 'predicted_value' in nd:
                        del nd['predicted_value']
                    nd[data_column] = data_values[i] if len(data_values) > i else None

                new_explanations = []
                explanaion = explanations[0][predict]
                original_target_values = {f'{predict}_original': [None] * nr_predictions}
                original_target_values[f'{predict}_original'][0] = explanaion.get('truth', None)
                for i in range(nr_predictions):
                    nd = {}
                    for key in explanaion:
                        if key not in ('predicted_value', 'confidence', 'confidence_upper_bound', 'confidence_lower_bound'):
                            nd[key] = explanaion[key]
                    for key in ('predicted_value', 'confidence', 'confidence_upper_bound', 'confidence_lower_bound'):
                        nd[key] = explanaion[key][i]
                    new_explanations.append({predict: nd})
                explanations = new_explanations
            else:
                # pred_dicts.reverse()
                for row in pred_dicts:
                    new_row = {}
                    new_row.update(row[predict])
                    new_row[predict] = row[predict]['predicted_value'][0]
                    new_row[data_column] = row[predict][data_column][0]
                    new_pred_dicts.append(new_row)
                for i in range(1, len(pred_dicts[-1][predict]['predicted_value'])):
                    new_row = {}
                    new_row.update(pred_dicts[-1][predict])
                    new_row[predict] = pred_dicts[-1][predict]['predicted_value'][i]
                    new_row[data_column] = pred_dicts[-1][predict][data_column][i]
                    new_pred_dicts.append(new_row)
                for row in new_pred_dicts:
                    if 'predicted_value' in row:
                        del row['predicted_value']
                # pred_dicts.reverse()
                # new_pred_dicts.reverse()

                new_explanations = []
                # explanations.reverse()
                original_values = []
                original_target_values = {f'{predict}_original': original_values}
                for expl in explanations:
                    explanaion = expl[predict]
                    original_values.append(explanaion.get('truth', None))
                    nd = {}
                    for key in ('predicted_value', 'confidence', 'confidence_upper_bound', 'confidence_lower_bound'):
                        nd[key] = explanaion[key][0]
                    new_explanations.append(nd)

                expl = explanations[-1]
                explanaion = expl[predict]
                for i in range(1, model['problem_definition']['timeseries_settings']['nr_predictions']):
                    nd = {}
                    original_values.append(None)
                    for key in ('predicted_value', 'confidence', 'confidence_upper_bound', 'confidence_lower_bound'):
                        nd[key] = explanaion[key][i]
                    new_explanations.append(nd)
                # explanations.reverse()
                # new_explanations.reverse()
                # original_values.reverse()
                new_explanations = [{predict: x} for x in new_explanations]

                # # explanaion = explanations[0][predict]
                # for i in range(model['problem_definition']['timeseries_settings']['nr_predictions']):
                #     nd = {}
                #     for key in explanaion:
                #         if key not in ('predicted_value', 'confidence', 'confidence_upper_bound', 'confidence_lower_bound'):
                #             nd[key] = explanaion[key]
                #     for key in ('predicted_value', 'confidence', 'confidence_upper_bound', 'confidence_lower_bound'):
                #         nd[key] = explanaion[key][i]
                #     new_explanations.append({predict: nd})
                # explanations = new_explanations

            pred_dicts = new_pred_dicts
            explanations = new_explanations

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
        for i, el in enumerate(pred_dicts):
            data.append({key: el[key] for key in keys})
            explains.append(explanations[i])

        for i, row in enumerate(data):
            cast_row_types(row, model['dtype_dict'])

            row['select_data_query'] = select_data_query
            row['external_datasource'] = external_datasource
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
