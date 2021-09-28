import mysql.connector

from lightwood.api import dtype
from mindsdb.integrations.base import Integration
from mindsdb.utilities.log import log


class MariadbConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.ssl = kwargs.get('ssl')
        self.ssl_ca = kwargs.get('ssl_ca')
        self.ssl_cert = kwargs.get('ssl_cert')
        self.ssl_key = kwargs.get('ssl_key')

    def check_connection(self):
        try:
            config = {
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "password": self.password
            }
            if self.ssl is True:
                config['client_flags'] = [mysql.connector.constants.ClientFlag.SSL]
                if self.ssl_ca is not None:
                    config["ssl_ca"] = self.ssl_ca
                if self.ssl_cert is not None:
                    config["ssl_cert"] = self.ssl_cert
                if self.ssl_key is not None:
                    config["ssl_key"] = self.ssl_key
            con = mysql.connector.connect(**config)
            connected = con.is_connected()
            con.close()
        except Exception:
            connected = False
        return connected


class Mariadb(Integration, MariadbConnectionChecker):
    def __init__(self, config, name, db_info):
        super().__init__(config, name)
        self.user = db_info.get('user', 'default')
        self.password = db_info.get('password', None)
        self.host = db_info.get('host')
        self.port = db_info.get('port')
        self.ssl = db_info.get('ssl')
        self.ssl_ca = db_info.get('ssl_ca')
        self.ssl_cert = db_info.get('ssl_cert')
        self.ssl_key = db_info.get('ssl_key')

    def _to_mariadb_table(self, dtype_dict, predicted_cols, columns):
        subtype_map = {
            dtype.integer: 'int',
            dtype.float: 'double',
            dtype.binary: 'bool',
            dtype.date: 'Date',
            dtype.datetime: 'Datetime',
            dtype.binary: 'VARCHAR(500)',
            dtype.categorical: 'VARCHAR(500)',
            dtype.tags: 'VARCHAR(500)',
            dtype.image: 'VARCHAR(500)',
            dtype.video: 'VARCHAR(500)',
            dtype.audio: 'VARCHAR(500)',
            dtype.short_text: 'VARCHAR(500)',
            dtype.rich_text: 'VARCHAR(500)',
            dtype.array: 'VARCHAR(500)'
        }

        column_declaration = []
        for name in columns:
            try:
                col_subtype = dtype_dict[name]
                new_type = subtype_map[col_subtype]
                column_declaration.append(f' `{name}` {new_type} ')
                if name in predicted_cols:
                    column_declaration.append(f' `{name}_original` {new_type} ')
            except Exception as e:
                log.error(f'Error: can not determine mariadb data type for column {name}: {e}')

        return column_declaration

    def _escape_table_name(self, name):
        return '`' + name.replace('`', '``') + '`'

    def _query(self, query):
        con = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
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
            ) ENGINE=CONNECT CHARSET=utf8 TABLE_TYPE=MYSQL CONNECTION='{connect}';
        """
        self._query(q)

        connect = self._get_connect_string('commands')

        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.commands (
                command VARCHAR(500)
            ) ENGINE=CONNECT CHARSET=utf8 TABLE_TYPE=MYSQL CONNECTION='{connect}';
        """
        self._query(q)

    def register_predictors(self, model_data_arr):
        for model_meta in model_data_arr:
            name = model_meta['name']
            predict = model_meta['predict']
            if not isinstance(predict, list):
                predict = [predict]
            columns_sql = ','.join(self._to_mariadb_table(
                model_meta['dtype_dict'],
                predict,
                list(model_meta['dtype_dict'].keys())
            ))
            columns_sql += ',`when_data` varchar(500)'
            columns_sql += ',`select_data_query` varchar(500)'
            columns_sql += ',`external_datasource` varchar(500)'
            for col in predict:
                columns_sql += f',`{col}_confidence` double'
                if model_meta['dtype_dict'][col] in (dtype.integer, dtype.float):
                    columns_sql += f',`{col}_min` double'
                    columns_sql += f',`{col}_max` double'
                columns_sql += f',`{col}_explain` varchar(500)'

            connect = self._get_connect_string(name)

            self.unregister_predictor(name)
            q = f"""
                    CREATE TABLE {self.mindsdb_database}.{self._escape_table_name(name)}
                    ({columns_sql}
                    ) ENGINE=CONNECT CHARSET=utf8 TABLE_TYPE=MYSQL CONNECTION='{connect}';
            """
            self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            drop table if exists {self.mindsdb_database}.{self._escape_table_name(name)};
        """
        self._query(q)
