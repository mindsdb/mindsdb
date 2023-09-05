from collections import OrderedDict
from typing import List

import pandas as pd
import psycopg
from mindsdb_sql import ASTNode, CreateTable, Insert, Select

from mindsdb.integrations.handlers.postgres_handler.postgres_handler import (
    PostgresHandler,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler


class PgVectorHandler(PostgresHandler, VectorStoreHandler):
    """This handler handles connection and execution of the PostgreSQL with pgvector extension statements."""

    name = "pgvector"

    def __init__(self, name: str, **kwargs):

        super().__init__(name=name, **kwargs)

    @profiler.profile()
    def connect(self):
        """
        Handles the connection to a PostgreSQL database instance.
        """
        self.connection = super().connect()

        with self.connection.cursor() as cur:
            try:
                # load pg_vector extension
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                self.connection.commit()

            except psycopg.Error as e:
                log.logger.error(
                    f"Error loading pg_vector extension, ensure you have installed it before running, {e}!"
                )

        return self.connection

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> HandlerResponse:

        with self.connection.cursor() as cur:
            try:
                # convert search embedding to string
                string_embeddings_search = str(query.where.args[1].items[0].value)
                # get limit from query
                limit = query.limit.value if query.limit else 5
                # we need to use the <-> operator to search for similar vectors,
                # so we need to convert the string to a vector and also use a threshold (e.g. 0.5)
                cur.execute(
                    f"SELECT * FROM {table_name} WHERE embedding <-> '{string_embeddings_search}' < 0.5 LIMIT {limit}"
                )
                self.connection.commit()
                result = cur.fetchall()
            except psycopg.Error as e:
                log.logger.error(f"Error creating table {collection_name}, {e}!")
                return Response(resp_type=RESPONSE_TYPE.ERROR, error_message=e)

        result = pd.DataFrame(result, columns=["id", "embeddings"])

        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=result)

    def create_collection(self, query: ASTNode) -> Response:
        """
        Run a create table query on the pgvector database.
        """
        collection_name = query.name.parts[-1]

        with self.connection.cursor() as cur:
            try:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {collection_name} (id bigserial PRIMARY KEY, embedding vector)"
                )
                self.connection.commit()
            except psycopg.Error as e:
                log.logger.error(f"Error creating table {collection_name}, {e}!")
                return Response(resp_type=RESPONSE_TYPE.ERROR, error_message=e)

        return Response(resp_type=RESPONSE_TYPE.OK)

    @profiler.profile()
    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        return VectorStoreHandler.query(self, query)  # to avoid diamond problem


connection_args = OrderedDict(
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the PostgreSQL server.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the PostgreSQL server.",
        "required": True,
        "label": "Password",
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "The database name to use when connecting with the PostgreSQL server.",
        "required": True,
        "label": "Database",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the PostgreSQL server. NOTE: use '127.0.0.1' instead of 'localhost' to connect to local server.",
        "required": True,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The TCP/IP port of the PostgreSQL server. Must be an integer.",
        "required": True,
        "label": "Port",
    },
    schema={
        "type": ARG_TYPE.STR,
        "description": "The schema in which objects are searched first.",
        "required": False,
        "label": "Schema",
    },
    sslmode={
        "type": ARG_TYPE.STR,
        "description": "sslmode that will be used for connection.",
        "required": False,
        "label": "sslmode",
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1", port=5432, user="root", password="password", database="database"
)
