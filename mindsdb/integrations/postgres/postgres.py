import pandas as pd
from psycopg_pool import ConnectionPool
from mindsdb_datasources.datasources.data_source import SQLDataSource
from mindsdb.utilities.log import log


class PostgresDS(SQLDataSource):
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.database = kwargs.get('database')

    def _get_connection(self):
        conn_pool = ConnectionPool(f'host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}')
        return conn_pool

    def check_connection(self):
        try:
            conection = self._get_connection()
            with conection.getconn() as con:
                cur = con.cursor()
                cur.execute('select 1;')
            connected = True
        except Exception as e:
            connected = False
        return connected

    def query(self, q):
        con = self._get_connection()
        with conection.getconn() as con:
            df = pd.read_sql(q, con=con)
            df.columns = [x if isinstance(x, str) else x.decode('utf-8') for x in df.columns]
            for col_name in df.columns:
                try:
                    df[col_name] = df[col_name].apply(lambda x: x if isinstance(x, str) else x.decode('utf-8'))
                except Exception:
                    pass
            return df, self._make_colmap(df)

    def name(self):
        return 'Postgres - {}'.format(self._query)