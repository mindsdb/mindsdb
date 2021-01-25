import mysql.connector

from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
from mindsdb.integrations.base import Integration


class Mariadb(Integration):
    def _to_mariadb_table(self, stats, predicted_cols):
        subtype_map = {
            DATA_SUBTYPES.INT: 'int',
            DATA_SUBTYPES.FLOAT: 'double',
            DATA_SUBTYPES.BINARY: 'bool',
            DATA_SUBTYPES.DATE: 'Date',
            DATA_SUBTYPES.TIMESTAMP: 'Datetime',
            DATA_SUBTYPES.SINGLE: 'VARCHAR(500)',
            DATA_SUBTYPES.MULTIPLE: 'VARCHAR(500)',
            DATA_SUBTYPES.TAGS: 'VARCHAR(500)',
            DATA_SUBTYPES.IMAGE: 'VARCHAR(500)',
            DATA_SUBTYPES.VIDEO: 'VARCHAR(500)',
            DATA_SUBTYPES.AUDIO: 'VARCHAR(500)',
            DATA_SUBTYPES.SHORT: 'VARCHAR(500)',
            DATA_SUBTYPES.RICH: 'VARCHAR(500)',
            DATA_SUBTYPES.ARRAY: 'VARCHAR(500)'
        }

        column_declaration = []
        for name in stats['columns']:
            try:
                col_subtype = stats[name]['typing']['data_subtype']
                new_type = subtype_map[col_subtype]
                column_declaration.append(f' `{name}` {new_type} ')
                if name in predicted_cols:
                    column_declaration.append(f' `{name}_original` {new_type} ')
            except Exception:
                print(f'Error: cant convert type {col_subtype} of column {name} to mariadb type')

        return column_declaration

    def _escape_table_name(self, name):
        return '`' + name.replace('`', '``') + '`'

    def _query(self, query):
        con = mysql.connector.connect(
            host=self.config['integrations'][self.name]['host'],
            port=self.config['integrations'][self.name]['port'],
            user=self.config['integrations'][self.name]['user'],
            password=self.config['integrations'][self.name]['password']
        )

        cur = con.cursor(dictionary=True, buffered=True)
        cur.execute(query)
        res = True
        try:
            res = cur.fetchall()
        except Exception:
            pass
        con.commit()
        con.close()

        return res

    def _get_connect_string(self, table):
        user = f"{self.config['api']['mysql']['user']}_{self.name}"
        password = self.config['api']['mysql']['password']
        host = self.config['api']['mysql']['host']
        port = self.config['api']['mysql']['port']

        if password is None or password == '':
            connect = f'mysql://{user}@{host}:{port}/mindsdb/{table}'
        else:
            connect = f'mysql://{user}:{password}@{host}:{port}/mindsdb/{table}'

        return connect

    def setup(self):
        self._query(f'DROP DATABASE IF EXISTS {self.mindsdb_database}')

        self._query(f'CREATE DATABASE IF NOT EXISTS {self.mindsdb_database}')

        connect = self._get_connect_string('predictors')

        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.predictors (
                name VARCHAR(500),
                status VARCHAR(500),
                accuracy VARCHAR(500),
                predict VARCHAR(500),
                select_data_query VARCHAR(500),
                external_datasource VARCHAR(500),
                training_options VARCHAR(500)
            ) ENGINE=CONNECT TABLE_TYPE=MYSQL CONNECTION='{connect}';
        """
        self._query(q)

        connect = self._get_connect_string('commands')

        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.commands (
                command VARCHAR(500)
            ) ENGINE=CONNECT TABLE_TYPE=MYSQL CONNECTION='{connect}';
        """
        self._query(q)

    def register_predictors(self, model_data_arr):
        for model_meta in model_data_arr:
            name = model_meta['name']
            stats = model_meta['data_analysis_v2']
            columns_sql = ','.join(self._to_mariadb_table(stats, model_meta['predict']))
            columns_sql += ',`when_data` varchar(500)'
            columns_sql += ',`select_data_query` varchar(500)'
            columns_sql += ',`external_datasource` varchar(500)'
            for col in model_meta['predict']:
                columns_sql += f',`{col}_confidence` double'
                if model_meta['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    columns_sql += f',`{col}_min` double'
                    columns_sql += f',`{col}_max` double'
                columns_sql += f',`{col}_explain` varchar(500)'

            connect = self._get_connect_string(name)

            q = f"""
                    CREATE TABLE {self.mindsdb_database}.{self._escape_table_name(name)}
                    ({columns_sql}
                    ) ENGINE=CONNECT TABLE_TYPE=MYSQL CONNECTION='{connect}';
            """
            self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            drop table if exists {self.mindsdb_database}.{self._escape_table_name(name)};
        """
        self._query(q)

    def check_connection(self):
        try:
            con = mysql.connector.connect(
                host=self.config['integrations'][self.name]['host'],
                port=self.config['integrations'][self.name]['port'],
                user=self.config['integrations'][self.name]['user'],
                password=self.config['integrations'][self.name]['password']
            )
            connected = con.is_connected()
            con.close()
        except Exception:
            connected = False
        return connected
