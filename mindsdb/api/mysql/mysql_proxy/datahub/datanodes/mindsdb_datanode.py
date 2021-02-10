import json
import pandas

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.interfaces.native.native import NativeInterface
from mindsdb.interfaces.custom.custom_models import CustomModels
from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.postgres.postgres import PostgreSQL
from mindsdb.integrations.mariadb.mariadb import Mariadb
from mindsdb.integrations.mysql.mysql import MySQL
from mindsdb.integrations.mssql.mssql import MSSQL
from mindsdb.utilities.functions import cast_row_types
from mindsdb_native.libs.helpers.general_helpers import NumpyJSONEncoder
from mindsdb.utilities.config import Config

class MindsDBDataNode(DataNode):
    type = 'mindsdb'

    def __init__(self, config):
        self.config = Config()
        self.mindsdb_native = NativeInterface()
        self.custom_models = CustomModels()

    def getTables(self):
        models = self.mindsdb_native.get_models()
        models = [x['name'] for x in models if x['status'] == 'complete']
        models += ['predictors', 'commands']
        models += [x['name'] for x in self.custom_models.get_models()]
        return models

    def hasTable(self, table):
        return table in self.getTables()

    def getTableColumns(self, table):
        try:
            columns = self.custom_models.get_model_data(table)['columns']
            columns += ['external_datasource', 'select_data_query', 'when_data']
            return columns
        except Exception:
            pass

        if table == 'predictors':
            return ['name', 'status', 'accuracy', 'predict', 'select_data_query', 'external_datasource', 'training_options']
        if table == 'commands':
            return ['command']

        model = self.mindsdb_native.get_model_data(name=table)
        columns = []
        columns += model['columns']
        columns += [f'{x}_original' for x in model['predict']]
        for col in model['predict']:
            if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                columns += [f"{col}_min", f"{col}_max"]
            columns += [f"{col}_confidence"]
            columns += [f"{col}_explain"]

        # TODO this should be added just for clickhouse queries
        columns += ['when_data', 'select_data_query', 'external_datasource']
        return columns

    def _select_predictors(self):
        models = self.mindsdb_native.get_models()
        # TODO add custom models
        return [{
            'name': x['name'],
            'status': x['status'],
            'accuracy': str(x['accuracy']) if x['accuracy'] is not None else None,
            'predict': ', '.join(x['predict']),
            'select_data_query': '',
            'external_datasource': '',  # TODO
            'training_options': ''  # TODO ?
        } for x in models]

    def delete_predictor(self, name):
        self.mindsdb_native.delete_model(name)

    def select(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None):
        ''' NOTE WHERE statements can be just $eq joined with 'and'
        '''
        if table == 'predictors':
            return self._select_predictors()
        if table == 'commands':
            return []

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
            elif dbtype == 'postgres':
                mysql = PostgreSQL(self.config, came_from)
                data = mysql._query(select_data_query)
            elif dbtype == 'mssql':
                mssql = MSSQL(self.config, came_from)
                data = mssql._query(select_data_query, fetch=True)
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

        try:
            model = self.custom_models.get_model_data(name=table)
        except Exception:
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

        if table in [x['name'] for x in self.custom_models.get_models()]:
            res = self.custom_models.predict(name=table, when_data=where_data)

            data = []
            fields = model['columns']
            for i, ele in enumerate(res):
                row = {}
                row['select_data_query'] = select_data_query
                row['external_datasource'] = external_datasource
                row['when_data'] = original_when_data

                for key in ele:
                    row[key] = ele[key]['predicted_value']
                    # FIXME prefer get int from mindsdb_native in this case
                    if model['data_analysis_v2'][key]['typing']['data_subtype'] == 'Int':
                        row[key] = int(row[key])

                for k in fields:
                    if k not in ele:
                        if isinstance(where_data, list):
                            if k in where_data[i]:
                                row[k] = where_data[i][k]
                            else:
                                row[k] = None
                        elif k in where_data.columns:
                            row[k] = where_data[k].iloc[i]
                        else:
                            row[k] = None

                for k in original_target_values:
                    row[k] = original_target_values[k][i]

                data.append(row)

            field_types = {f: model['data_analysis_v2'][f]['typing']['data_subtype'] for f in fields if 'typing' in model['data_analysis_v2'][f]}
            for row in data:
                cast_row_types(row, field_types)

            return data
        else:
            res = self.mindsdb_native.predict(name=table, when_data=where_data)

            keys = [x for x in list(res._data.keys()) if x in columns]
            min_max_keys = []
            for col in predicted_columns:
                if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    min_max_keys.append(col)

            data = []
            explains = []
            for i, el in enumerate(res):
                data.append({key: el[key] for key in keys})
                explains.append(el.explain())

            field_types = {
                f: model['data_analysis_v2'][f]['typing']['data_subtype']
                for f in model['columns'] if 'typing' in model['data_analysis_v2'][f]
            }

            for row in data:
                cast_row_types(row, field_types)

                row['select_data_query'] = select_data_query
                row['external_datasource'] = external_datasource
                row['when_data'] = original_when_data

                for k in original_target_values:
                    row[k] = original_target_values[k][i]

                explanation = explains[i]
                for key in predicted_columns:
                    row[key + '_confidence'] = explanation[key]['confidence']
                    row[key + '_explain'] = json.dumps(explanation[key], cls=NumpyJSONEncoder)
                for key in min_max_keys:
                    row[key + '_min'] = min(explanation[key]['confidence_interval'])
                    row[key + '_max'] = max(explanation[key]['confidence_interval'])

            return data
