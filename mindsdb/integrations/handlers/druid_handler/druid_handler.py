from typing import Optional
from collections import OrderedDict

import pandas as pd
from pydruid.db import connect

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from pinotdb.sqlalchemy import PinotDialect

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class DruidHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Apache Pinot statements.
    """

    name = 'druid'

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
        self.dialect = 'pinot'

        optional_parameters = ['user', 'password']
        for parameter in optional_parameters:
            if parameter not in connection_data:
                connection_data[parameter] = None

        if 'path' not in connection_data:
            connection_data['path'] = '/druid/v2/sql/'

        if 'scheme' not in connection_data:
            connection_data['scheme'] = 'http'

        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = connect(
            host=self.connection_data['host'],
            port=self.connection_data['port'],
            path=self.connection_data['path'],
            scheme=self.connection_data['scheme'],
            username=self.connection_data['user'],
            password=self.connection_data['password']
        )
        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.error(f'Error connecting to Pinot, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response