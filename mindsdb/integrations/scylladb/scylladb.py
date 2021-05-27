from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class ScyllaDBConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.keyspace = kwargs.get('database')

    def check_connection(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username=self.user, password=self.password
            )
            cluster = Cluster([self.host], port=self.port, auth_provider=auth_provider)
            session = cluster.connect()

            if isinstance(self.keyspace, str) and len(self.keyspace) > 0:
                session.set_keyspace(self.keyspace)

            session.execute('SELECT COUNT (1) FROM system.local;').all()

            connected = True
        except Exception:
            connected = False
        return connected
