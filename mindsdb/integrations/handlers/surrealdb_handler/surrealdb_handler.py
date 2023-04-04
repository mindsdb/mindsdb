from collections import OrderedDict
from typing import Optional
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

import pysurrealdb as surreal
import pandas as pd


class SurrealDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SurrealDB statements.
    """

    name = 'surrealdb'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.database = connection_data['database']
        self.parser = parse_sql
        self.dialect = "surrealdb"
        self.kwargs = kwargs
        self.namespace = connection_data['namespace']
        self.user = connection_data['user']
        self.password = connection_data['password']
        self.host = connection_data['host']
        self.port = connection_data['port']

        self.connection = None
        self.is_connected = False

    def connect(self) -> StatusResponse:
        """
        Establishes a connection to the MindsDB database.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.connection
        try:
            self.connection = surreal.connect(
                database=self.database,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                namespace=self.namespace,
            )
            self.is_connected = True
        except Exception as e:
            log.logger.error(f"Error while connecting to SurrealDB, {e}")

        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            log.logger.error(f"Error while disconnecting to SurrealDB, {e}")

        return

    def disconnect(self):
        """
        Close the existing connection to the SurrealDB database
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            log.error(f"Error while disconnecting to SurrealDB, {e}")

        return self.is_connected

    def native_query(self, query: str) -> HandlerResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (Any): query in SurrealQL to execute
        Returns:
            HandlerResponse
        """

    def query(self, query: ASTNode) -> HandlerResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

    def get_tables(self) -> HandlerResponse:
        """
        Get list of tables from the database that will be accessible.
        Returns:
            HandlerResponse
        """

    def get_columns(self, table: str) -> HandlerResponse:
        """ Return list of columns in table
        Args:
            table (str): name of the table to get column names and types from.
        Returns:
            HandlerResponse
        """


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the SurrealDB server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the SurrealDB server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the SurrealDB server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the SurrealDB server. '
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the SurrealDB server. Must be an integer.'
    },
    namespace={
        'type': ARG_TYPE.STR,
        'description': ''
    }
)
connection_args_example = OrderedDict(
    host='localhost',
    port=8000,
    user='admin',
    password='password',
    database='test',
    namespace='test'
)
