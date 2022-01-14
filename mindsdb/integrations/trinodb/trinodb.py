from mindsdb_datasources import TrinoDS


class TrinodbConnectionChecker:
    def __init__(self, **kwargs):
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.catalog = kwargs.get('catalog')
        self.schema = kwargs.get('schema')

    def check_connection(self):
        try:
            ds = TrinoDS('SELECT 1', self.user, self.password,  self.host, self.port,  self.catalog, self.schema )
            assert len(ds.df) == 1
        except Exception as e:
            return False
        else:
            return True
