import requests

from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
from mindsdb.integrations.base import Integration


class Clickhouse(Integration):
    def _to_clickhouse_table(self, stats, predicted_cols):
        subtype_map = {
            DATA_SUBTYPES.INT: 'Nullable(Int64)',
            DATA_SUBTYPES.FLOAT: 'Nullable(Float64)',
            DATA_SUBTYPES.BINARY: 'Nullable(UInt8)',
            DATA_SUBTYPES.DATE: 'Nullable(Date)',
            DATA_SUBTYPES.TIMESTAMP: 'Nullable(Datetime)',
            DATA_SUBTYPES.SINGLE: 'Nullable(String)',
            DATA_SUBTYPES.MULTIPLE: 'Nullable(String)',
            DATA_SUBTYPES.IMAGE: 'Nullable(String)',
            DATA_SUBTYPES.VIDEO: 'Nullable(String)',
            DATA_SUBTYPES.AUDIO: 'Nullable(String)',
            DATA_SUBTYPES.SHORT: 'Nullable(String)',
            DATA_SUBTYPES.RICH: 'Nullable(String)',
            DATA_SUBTYPES.ARRAY: 'Nullable(String)'
        }

        column_declaration = []
        for name in stats['columns']:
            try:
                col_subtype = stats[name]['typing']['data_subtype']
                new_type = subtype_map[col_subtype]
                column_declaration.append(f' `{name}` {new_type} ')
                if name in predicted_cols:
                    column_declaration.append(f' `{name}_original` {new_type} ')
            except Exception as e:
                print(e)
                print(f'Error: cant convert type {col_subtype} of column {name} to clickhouse type')

        return column_declaration

    def _query(self, query):
        params = {'user': 'default'}
        try:
            params['user'] = self.config['integrations'][self.name]['user']
        except Exception:
            pass

        try:
            params['password'] = self.config['integrations'][self.name]['password']
        except Exception:
            pass

        host = self.config['integrations'][self.name]['host']
        port = self.config['integrations'][self.name]['port']

        response = requests.post(f'http://{host}:{port}', data=query, params=params)

        if response.status_code != 200:
            raise Exception(f'Error: {response.content}\nQuery:{query}')

        return response

    def _get_mysql_user(self):
        return f"{self.config['api']['mysql']['user']}_{self.name}"

    def _escape_table_name(self, name):
        return '`' + name.replace('`', '\\`') + '`'

    def setup(self):
        self._query(f'DROP DATABASE IF EXISTS {self.mindsdb_database}')
        self._query(f'CREATE DATABASE IF NOT EXISTS {self.mindsdb_database}')

        msqyl_conn = self.config['api']['mysql']['host'] + ':' + str(self.config['api']['mysql']['port'])
        msqyl_pass = self.config['api']['mysql']['password']
        msqyl_user = self._get_mysql_user()

        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.predictors (
                name String,
                status String,
                accuracy String,
                predict String,
                select_data_query String,
                external_datasource String,
                training_options String
                ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', 'predictors', '{msqyl_user}', '{msqyl_pass}')
        """
        self._query(q)
        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.commands (
                command String
            ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', 'commands', '{msqyl_user}', '{msqyl_pass}')
        """
        self._query(q)

    def register_predictors(self, model_data_arr):
        for model_meta in model_data_arr:
            name = self._escape_table_name(model_meta['name'])
            stats = model_meta['data_analysis']

            columns_sql = ','.join(self._to_clickhouse_table(stats, model_meta['predict']))
            columns_sql += ',`when_data` Nullable(String)'
            columns_sql += ',`select_data_query` Nullable(String)'
            columns_sql += ',`external_datasource` Nullable(String)'
            for col in model_meta['predict']:
                columns_sql += f',`{col}_confidence` Nullable(Float64)'
                
                if model_meta['data_analysis'][col]['typing']['data_type'] == 'Numeric':
                    columns_sql += f',`{col}_min` Nullable(Float64)'
                    columns_sql += f',`{col}_max` Nullable(Float64)'
                columns_sql += f',`{col}_explain` Nullable(String)'

            msqyl_conn = self.config['api']['mysql']['host'] + ':' + str(self.config['api']['mysql']['port'])
            msqyl_pass = self.config['api']['mysql']['password']
            msqyl_user = self._get_mysql_user()

            q = f"""
                CREATE TABLE {self.mindsdb_database}.{name}
                ({columns_sql}
                ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', {name}, '{msqyl_user}', '{msqyl_pass}')
            """
            self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            drop table if exists {self.mindsdb_database}.{self._escape_table_name(name)};
        """
        self._query(q)

    def check_connection(self):
        try:
            res = self._query('select 1;')
            connected = res.status_code == 200
        except Exception:
            connected = False
        return connected
