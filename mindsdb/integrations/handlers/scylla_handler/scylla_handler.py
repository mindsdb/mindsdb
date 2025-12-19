from mindsdb.integrations.handlers.cassandra_handler.cassandra_handler import CassandraHandler


class ScyllaHandler(CassandraHandler):
    """
    This handler handles connection and execution of the Scylla statements.
    """
    name = 'scylla'

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
