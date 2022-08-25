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