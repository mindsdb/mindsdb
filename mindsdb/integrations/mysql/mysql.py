from contextlib import closing
import mysql.connector as mysqlc
from mindsdb.integrations.data_source import SQLDataSource

from lightwood.api import dtype
from mindsdb.utilities.log import log


class MySqlDS(SQLDataSource):
    """
    SQL Datasource class used for connections to MySQL
    TODO: Combine MySqlDS and MariaDS
    """
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.ssl = kwargs.get('ssl')
        self.ssl_ca = kwargs.get('ssl_ca')
        self.ssl_cert = kwargs.get('ssl_cert')
        self.ssl_key = kwargs.get('ssl_key')

    def _get_connection(self):
        """
        TODO: re use the same connection instead of open/close per query
        """
        config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password
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

