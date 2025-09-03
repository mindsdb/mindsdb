import psycopg
from pgvector.psycopg import register_vector
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)
from mindsdb.integrations.handlers.pgvector_handler.pgvector_handler import (
    PgVectorHandler,
)
from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse
from mindsdb.utilities.profiler import profiler
class OpenGaussHandler(PgVectorHandler):
    """
    This handler handles connection and execution of the openGauss statements.
    """
    name = 'opengauss'
    
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, **kwargs)

    @profiler.profile()
    def connect(self) -> psycopg.connection:
        """
        Handles the connection to a PostgreSQL database instance.
        """
        self.connection = PostgresHandler.connect(self)
        if self._is_vector_registered:
            return self.connection
        
        # we do not need to create extesion as vector support is integrated in opoengauss since 7.0.0
        # register vector type with psycopg2 connection
        register_vector(self.connection)
        self._is_vector_registered = True
        return self.connection
    
    def insert(self, table_name, data) -> HandlerResponse:
        super().insert(table_name, data)
        return HandlerResponse(RESPONSE_TYPE.OK)