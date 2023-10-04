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

    def __init__(self, name: str = None, **kwargs: dict):
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
                "port": self.connection_data.get("port"),
                "serialization_type": self.connection_data.get("serialization_type"),
            }

            try:
                self.connection = OrientDB(**client_config)
            except Exception as e:
                log.logger.error(f"Error in creating OrientDB client: {e}")

            database_config = {
                "db_name": self.connection_data.get("database"),
                "user": self.connection_data.get("user"),
                "password": self.connection_data.get("password"),
            }

            try:
                self.connection.db_open(**database_config)
            except Exception as e:
                log.logger.error(
                    f'Error connecting to OrientDB database: {self.connection_data.get("database")}, {e}!'
                )

            self.is_connected = True
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return

        self.connection.db_close()
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = self.is_connected
        except Exception as e:
            log.logger.error(
                f'Error connecting to OrientDB database: {self.connection_data.get("database")}, {e}!'
            )
            response.error_message = str(e)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        need_to_close = self.is_connected is False
        connection = self.connect()

        try:
            semicolon_count = query.count(";")
            if semicolon_count > 1:
                query = "BEGIN;\n" + query + "\nCOMMIT RETRY 100;"
                results = connection.batch(query)
            else:
                results = connection.command("query")

            data = [r.oRecordData for r in results]
            response = Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(data))

        except Exception as e:
            log.logger.error(
                f'Error running query: {query} on {self.connection_data["database"]}.'
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        return super().query(query)

    def get_columns(self, table_name: str) -> Response:
        return super().get_columns(table_name)

    def get_tables(self) -> Response:
        default_tables = [
            "OIdentity",
            "ORole",
            "V",
            "E",
            "OFunction",
            "OSchedule",
            "OUser",
            "OTriggered",
        ]

        query = "SELECT expand(classes) FROM metadata:schema"
        connection = self.connect()
        tables = connection.command(query)

        db_name = f"Tables_in_{self.collection_data['database']}"
        data = {db_name: []}

        for t in tables:
            record_data = t.oRecordData
            record_name = record_data["name"]
            if record_name not in default_tables:
                data[db_name].append(record_name)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(data),
        )
        return response


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
