import os

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class CassandraConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.keyspace = kwargs.get('database')
        self.secure_connect_bundle = kwargs.get('secure_connect_bundle')
        self.protocol_version = kwargs.get('protocol_version')

    def check_connection(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username=self.user, password=self.password
            )
            connection_props = {
                'auth_provider': auth_provider
            }

            if self.protocol_version is not None:
                connection_props['protocol_version'] = self.protocol_version

            if self.secure_connect_bundle is not None:
                if os.path.isfile(self.secure_connect_bundle) is False:
                    raise Exception("'secure_connect_bundle' must be path to the file")
                connection_props['cloud'] = {
                    'secure_connect_bundle': self.secure_connect_bundle
                }
            else:
                connection_props['contact_points'] = [self.host]
                connection_props['port'] = self.port

            cluster = Cluster(**connection_props)
            session = cluster.connect()

            if isinstance(self.keyspace, str) and len(self.keyspace) > 0:
                session.set_keyspace(self.keyspace)

            session.execute('SELECT release_version FROM system.local;').one()

            connected = True
        except Exception:
            connected = False
        return connected
