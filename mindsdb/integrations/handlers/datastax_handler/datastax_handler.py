from ..scylla_handler import Handler as ScyllaHandler, connection_args

class DatastaxHandler(ScyllaHandler):
    """
    This handler handles connection and execution of the Datastax Astra DB statements.
    """
    name = 'astra'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
