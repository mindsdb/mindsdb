from mindsdb_datasources import SnowflakeDS


class SnowflakeConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.account = kwargs.get('account')
        self.warehouse = kwargs.get('warehouse')
        self.database = kwargs.get('database')
        self.schema = kwargs.get('schema')
        self.protocol = kwargs.get('protocol')
        self.port = kwargs.get('port')

    def check_connection(self):
        try:
            ds = SnowflakeDS('SELECT 1;', self.host, self.user, self.password, self.account, self.warehouse, self.database, self.schema, self.protocol, self.port)
            assert len(ds.df) == 1
            connected = True
        except Exception as e:
            try:
                print(e)
                connected = False
            finally:
                e = None
                del e

        else:
            return connected
