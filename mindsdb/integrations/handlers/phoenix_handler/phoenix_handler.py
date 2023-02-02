from typing import Optional
from collections import OrderedDict

import pandas as pd
import phoenixdb
import requests
from requests.exceptions import InvalidSchema
import json

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class PhoenixHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Apache Phoenix statements.
    """

    name = 'phoenix'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'phoenix'

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        pass

    def disconnect(self):
        """ Close any existing connections

        Should switch self.is_connected.
        """
        pass

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        pass

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        pass

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        pass

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        pass

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        pass


connection_args = OrderedDict(

)

connection_args_example = OrderedDict(

)
