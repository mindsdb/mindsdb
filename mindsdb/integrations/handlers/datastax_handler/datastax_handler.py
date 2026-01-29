from mindsdb.integrations.handlers.cassandra_handler.cassandra_handler import CassandraHandler


class DatastaxHandler(CassandraHandler):
    """
    This handler handles connection and execution of the Datastax Astra DB statements.
    """
    name = 'astra'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
