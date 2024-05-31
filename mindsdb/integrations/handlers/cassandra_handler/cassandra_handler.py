from mindsdb.integrations.handlers.scylla_handler import Handler as ScyllaHandler


class CassandraHandler(ScyllaHandler):
    """
    This handler handles connection and execution of the Cassandra statements.
    """

    name = 'cassandra'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
