import os
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler

os.environ["PGCLIENTENCODING"] = "utf-8"


class RedshiftHandler(PostgresHandler):
    """
    This handler handles connection and execution of the Redshift statements.
    """

    name = 'redshift'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name,**kwargs)

