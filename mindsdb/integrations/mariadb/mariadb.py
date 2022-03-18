import pandas as pd
from pandas.io.sql import DatabaseError 
import mysql.connector as mysqlc

from mindsdb.integrations.data_source import SQLDataSource
from lightwood.api import dtype
from mindsdb.utilities.log import log


class MariaDS(SQLDataSource):
    """
    SQL Datasource class used for connections to MariaDB
    """

    def __init__(self, query=None, database='mysql', host='localhost',
                 port=3306, user='root', password='', ssl=None, 
                 ssl_ca=None, ssl_cert=None, ssl_key=None, publish=None, 
                 type='mariadb', integrations_name=None):
        super().__init__(query)
        self.database = database
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.ssl = ssl
        self.ssl_ca = ssl_ca
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        
    def _get_connection(self):
        """
        TODO: re use the same connection instead of open/close per query
        """
        config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database
        }
        if self.ssl is True:
            config['client_flags'] = [mysqlc.constants.ClientFlag.SSL]
            if self.ssl_ca is not None:
                config["ssl_ca"] = self.ssl_ca
            if self.ssl_cert is not None:
                config["ssl_cert"] = self.ssl_cert
            if self.ssl_key is not None:
                config["ssl_key"] = self.ssl_key
        return mysqlc.connect(**config)

    def query(self, q):
        try:
            connection = self._get_connection()
            with connection as cnn:
                df = pd.read_sql(q, con=cnn)
        except mysqlc.Error as err:
            log.error(f'Something went wrong: {err}')
        return df, self._make_colmap(df)

    def check_connection(self):
        try:
            connection = self._get_connection()
            with connection as cnn:
                connected = cnn.is_connected()
        except mysqlc.Error as err:
            log.error(f'Something went wrong: {err}')
        return connected

    def name(self):
        return 'MariaDB - {}'.format(self._query)

