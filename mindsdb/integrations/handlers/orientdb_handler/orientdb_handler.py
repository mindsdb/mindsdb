from collections import OrderedDict
from typing import Any

import pandas as pd
from pyorient import OrientDB

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log


class OrientDBHandler(DatabaseHandler):
    name = "orientdb"

    def __init__(self, name: str = None, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data")

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        if self.is_connected is False:
            client_config = {
                "host": self.connection_data.get("host"),
                "port": self.connection.get("port"),
                "serialization_type": self.connection_data.get("serialization_type"),
            }

            try:
                client = OrientDB(**client_config)
            except Exception as e:
                log.logger.error(f"Error in creating OrientDB client: {e}")

            connection_config = {
                "db_name": self.connection_data.get("database"),
                "user": self.connection_data.get("user"),
                "password": self.connection_data.get("password"),
            }

            try:
                connection = client.db_open(**connection_config)
            except Exception as e:
                log.logger.error(
                    f'Error connecting to {self.connection_data.get("database")}, {e}!'
                )

            self.connection = connection
            self.is_connected = True
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            con = self.connect()
            con.server_info()
            response.success = True
        except Exception as e:
            log.logger.error(
                f'Error connecting to {self.connection_data.get("database")}, {e}!'
            )
            response.error_message = str(e)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: Any) -> Response:
        ...

    def query(self, query: ASTNode) -> Response:
        ...

    def get_columns(self, table_name: str) -> Response:
        ...

    def get_tables(self) -> Response:
        ...


connection_args = OrderedDict(
    user={
        "type": ARG_TYPE.STR,
        "description": "The username used to authenticate the user when connecting to the OrientDB server.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.STR,
        "description": "The password used to authenticate the user when connecting to the OrientDB server.",
        "required": True,
        "label": "Password",
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "The name of the database on the OrientDB to connect to.",
        "required": True,
        "label": "Database",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the Orient server. NOTE: use '127.0.0.1' instead of 'localhost' to connect to local server.",
        "required": True,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The TCP/IP port number for the OrientDB server. Must be an integer.",
        "required": True,
        "label": "Port",
    },
    serialization_type={
        "type": ARG_TYPE.STR,
        "description": "The serialization type to use for communication.",
        "required": False,
        "label": "Serialization Type",
    },
)


connection_args_example = OrderedDict(
    host="127.0.0.1",
    port=2424,
    user="root",
    password="password",
    database="database",
)
