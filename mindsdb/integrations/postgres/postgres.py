from contextlib import closing
import psycopg


class PostgreSQLConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.database = kwargs.get('database', 'postgres')

    def _get_connection(self):
        conn = psycopg.connect(f'host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}', connect_timeout=10)
        return conn

    def check_connection(self):
        try:
            con = self._get_connection()
            with closing(con) as con:
                cur = con.cursor()
                cur.execute('select 1;')
            connected = True
        except Exception as e:
            print('EXCEPTION!')
            print(e)
            connected = False
        return connected
