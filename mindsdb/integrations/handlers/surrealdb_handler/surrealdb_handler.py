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

    def connect(self) -> HandlerStatusResponse:
        """
        Establishes a connection to the MindsDB database.
        Returns:
            HandlerStatusResponse
        """

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

    def disconnect(self):
        """
        Close the existing connection to the SurrealDB database
        """

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