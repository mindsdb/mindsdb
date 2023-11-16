from mindsdb.integrations.handlers.scylla_handler import Handler as ScyllaHandler


class DatastaxHandler(ScyllaHandler):
    """
    This handler handles connection and execution of the Datastax Astra DB statements.
    """

    name = "astra"

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
