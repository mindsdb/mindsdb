from mindsdb.integrations.handlers.postgres_handler import Handler as PostgresHandler


class OpenGaussHandler(PostgresHandler):
    """
    This handler handles connection and execution of the openGauss statements.
    """
    name = 'opengauss'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
