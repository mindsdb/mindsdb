import json
import datetime
import pandas

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.mariadb.mariadb import Mariadb
from mindsdb.integrations.mysql.mysql import MySQL


class MindsDBDataNode(DataNode):
    type = 'mindsdb'

    def __init__(self, config):
        self.config = config
        self.mindsdb_native = MindsdbNative(config)

    def getTables(self):
        models = self.mindsdb_native.get_models()
        models = [x['name'] for x in models if x['status'] == 'complete']
        models += ['predictors']
        return models

    def hasTable(self, table):
        return table in self.getTables()

    def getTableColumns(self, table):
        if table == 'predictors':
            return ['name', 'status', 'accuracy', 'predict', 'select_data_query', 'external_datasource', 'training_options']
        if table == 'commands':
            return ['command']
        model = self.mindsdb_native.get_model_data(name=table)
        columns = []
        columns += [x['column_name'] for x in model['data_analysis']['input_columns_metadata']]
        columns += [x['column_name'] for x in model['data_analysis']['target_columns_metadata']]
        columns += [f'{x}_original' for x in model['predict']]
        for col in model['predict']:
            columns += [f"{col}_confidence"]
            if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                columns += [f"{col}_min", f"{col}_max"]
            columns += [f"{col}_explain"]

        # TODO this should be added just for clickhouse queries
        columns += ['select_data_query']
        columns += ['external_datasource']
        return columns

    def _select_predictors(self):
        models = self.mindsdb_native.get_models()
        return [{
            'name': x['name'],
            'status': x['status'],
            'accuracy': x['accuracy'],
            'predict': ', '.join(x['predict']),
            'select_data_query': x['data_source'],
            'external_datasource': '', # TODO
            'training_options': ''  # TODO ?
        } for x in models]

    def delete_predictor(self, name):
        self.mindsdb_native.delete_model(name)

    def select(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None):
        ''' NOTE WHERE statements can be just $eq joined with 'and'
        '''
        if table == 'predictors':
            return self._select_predictors()

        external_datasource = None
        if 'external_datasource' in where:
            external_datasource = where['external_datasource']['$eq']
            del where['external_datasource']

        select_data_query = None
        if came_from is not None and 'select_data_query' in where:
            select_data_query = where['select_data_query']['$eq']
            del where['select_data_query']

            dbtype = self.config['integrations'][came_from]['type']
            if dbtype == 'clickhouse':
                ch = Clickhouse(self.config, came_from)
                res = ch._query(select_data_query.strip(' ;\n') + ' FORMAT JSON')
                data = res.json()['data']
            elif dbtype == 'mariadb':
                maria = Mariadb(self.config, came_from)
                data = maria._query(select_data_query)
            elif dbtype == 'mysql':
                mysql = MySQL(self.config, came_from)
                data = mysql._query(select_data_query)
            else:
                raise Exception(f'Unknown database type: {dbtype}')

            if where_data is None:
                where_data = data
            else:
                where_data += data

        new_where = {}
        if where_data is not None:
            where_data = pandas.DataFrame(where_data)
        else:
            for key, value in where.items():
                if isinstance(value, dict) is False or len(value.keys()) != 1 or list(value.keys())[0] != '$eq':
                    # TODO value should be just string or number
                    raise Exception()
                new_where[key] = value['$eq']

            if len(new_where) == 0:
                return []

            where_data = [new_where]

        model = self.mindsdb_native.get_model_data(name=table)
        predicted_columns = model['predict']

        original_target_values = {}
        for col in predicted_columns:
            if where_data is not None:
                if col in where_data:
                    original_target_values[col + '_original'] = list(where_data[col])
                else:
                    original_target_values[col + '_original'] = [None] * len(where_data)
            else:
                original_target_values[col + '_original'] = [None]

        res = self.mindsdb_native.predict(name=table, when_data=where_data)

        data = []
        keys = [x for x in list(res._data.keys()) if x in columns]
        min_max_keys = []
        for col in predicted_columns:
            if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                min_max_keys.append(col)

        length = len(res._data[predicted_columns[0]])
        for i in range(length):
            row = {}
            explanation = res[i].explain()
            for key in keys:
                row[key] = res._data[key][i]
                # +++ FIXME this fix until issue https://github.com/mindsdb/mindsdb/issues/591 not resolved
                if key in model['data_analysis_v2'] and model['data_analysis_v2'][key]['typing']['data_subtype'] == 'Timestamp' and row[key] is not None:
                    timestamp = datetime.datetime.fromtimestamp(row[key])
                    row[key] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                # ---
            for key in predicted_columns:
                row[key + '_confidence'] = explanation[key]['confidence']
                row[key + '_explain'] = json.dumps(explanation[key])
            for key in min_max_keys:
                row[key + '_min'] = explanation[key]['confidence_interval'][0]
                row[key + '_max'] = explanation[key]['confidence_interval'][-1]
            row['select_data_query'] = select_data_query
            row['external_datasource'] = external_datasource
            for k in original_target_values:
                row[k] = original_target_values[k][i]
            data.append(row)

        return data
