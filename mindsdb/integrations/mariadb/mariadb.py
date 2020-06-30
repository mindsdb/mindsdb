import requests

import mysql.connector

from mindsdb_native.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES


class Mariadb():
    def __init__(self, config, name):
        self.config = config
        self.name = name
        self.host = config['integrations'][name]['host']
        self.port = config['integrations'][name]['port']
        self.user = config['integrations'][name]['user']
        self.password = config['integrations'][name]['password']

    def _to_mariadb_table(self, stats):
        subtype_map = {
            DATA_SUBTYPES.INT: 'int',
            DATA_SUBTYPES.FLOAT: 'double',
            DATA_SUBTYPES.BINARY: 'bool',
            DATA_SUBTYPES.DATE: 'Date',
            DATA_SUBTYPES.TIMESTAMP: 'Datetime',
            DATA_SUBTYPES.SINGLE: 'VARCHAR(500)',
            DATA_SUBTYPES.MULTIPLE: 'VARCHAR(500)',
            DATA_SUBTYPES.IMAGE: 'VARCHAR(500)',
            DATA_SUBTYPES.VIDEO: 'VARCHAR(500)',
            DATA_SUBTYPES.AUDIO: 'VARCHAR(500)',
            DATA_SUBTYPES.TEXT: 'VARCHAR(500)',
            DATA_SUBTYPES.ARRAY: 'VARCHAR(500)'
        }

        column_declaration = []
        for name, column in stats.items():
            try:
                col_subtype = stats[name]['typing']['data_subtype']
                new_type = subtype_map[col_subtype]
                column_declaration.append(f' `{name}` {new_type} ')
            except Exception as e:
                print(f'Error: cant convert type {col_subtype} of column {name} to mariadb tpye')

        return column_declaration

    def _query(self, query):
        con = mysql.connector.connect(host=self.host, port=self.port, user=self.user, password=self.password)

        cur = con.cursor(dictionary=True,buffered=True)
        cur.execute(query)
        res = True
        try:
            res = cur.fetchall()
        except:
            pass
        con.commit()
        con.close()

        return res

    def _get_connect_string(self, table):
        user = self.config['api']['mysql']['user']
        password = self.config['api']['mysql']['password']
        host = self.config['api']['mysql']['host']
        port = self.config['api']['mysql']['port']

        if password is None or password == '':
            connect = f'mysql://{user}@{host}:{port}/mindsdb/{table}'
        else:
            connect = f'mysql://{user}:{password}@{host}:{port}/mindsdb/{table}'

        return connect

    def setup(self, model_data_arr):
        self._query('DROP DATABASE IF EXISTS mindsdb')

        self._query('CREATE DATABASE IF NOT EXISTS mindsdb')

        connect = self._get_connect_string('predictors_mariadb')

        q = f"""
                CREATE TABLE IF NOT EXISTS mindsdb.predictors
                (name VARCHAR(500),
                status VARCHAR(500),
                accuracy VARCHAR(500),
                predict_cols VARCHAR(500),
                select_data_query VARCHAR(500),
                training_options VARCHAR(500)
                ) ENGINE=CONNECT TABLE_TYPE=MYSQL CONNECTION='{connect}';
        """
        print(f'Executing table creation query to create predictors list:\n{q}\n')
        self._query(q)

        connect = self._get_connect_string('commands_mariadb')

        q = f"""
            CREATE TABLE IF NOT EXISTS mindsdb.commands (
                command VARCHAR(500)
            ) ENGINE=CONNECT TABLE_TYPE=MYSQL CONNECTION='{connect}';
        """
        print(f'Executing table creation query to create command table:\n{q}\n')
        self._query(q)

        for model_meta in model_data_arr:
            self.register_predictor(model_meta)

    def register_predictors(self, model_meta):
        name = model_meta['name']
        stats = model_meta['data_analysis']
        columns_sql = ','.join(self._to_mariadb_table(stats))
        columns_sql += ',`$select_data_query` varchar(500)'
        for col in model_meta['predict_cols']:
            columns_sql += f',`${col}_confidence` double'

        connect = self._get_connect_string(f'{name}_mariadb')

        q = f"""
                CREATE TABLE mindsdb.{name}
                ({columns_sql}
                ) ENGINE=CONNECT TABLE_TYPE=MYSQL CONNECTION='{connect}';
        """
        print(f'Executing table creation query to sync predictor:\n{q}\n')
        self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            drop table if exists mindsdb.{name};
        """
        print(f'Executing table creation query to sync predictor:\n{q}\n')
        self._query(q)

    def check_connection(self):
        try:
            con = mysql.connector.connect(host=self.host, port=self.port, user=self.user, password=self.password)
            connected = con.is_connected()
            con.close()
        except Exception:
            connected = False
        return connected
