from mindsdb.integrations.handlers.postgres_handler import Handler as PostgresHandler


class KineticaHandler(PostgresHandler):
    """
    This handler handles connection and execution of the Kinetica statements.
    """

    name = 'kinetica'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
