from mindsdb.integrations.handlers.postgres_handler import Handler as PostgresHandler


class OrioleDBHandler(PostgresHandler):
    """
    This handler handles connection and execution of the OrioleDB statements.
    """
    name = 'orioledb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
