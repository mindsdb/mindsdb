from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler


class CockroachHandler(PostgresHandler):
    """
    This handler handles connection and execution of the Cockroachdb statements.
    """
    name = 'cockroachdb'

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
