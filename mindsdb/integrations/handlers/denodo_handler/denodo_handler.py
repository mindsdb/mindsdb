from typing import Optional
import pandas as pd
import pyodbc
from collections import OrderedDict
from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

class DenodoHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Denodo statements.
    """

    name = 'denodo'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'denodo'
        self.kwargs = kwargs
        self.connection_data = connection_data
        self.connection = None
        self.is_connected = False

    def connect(self):
        if self.is_connected is True:
            return self.connection

        config = {
            'DRIVER': self.connection_data.get('driver'),
            'URL': self.connection_data.get('url'),
            'UID': self.connection_data.get('user'),
            'PWD': self.connection_data.get('password')
        }

        connection = pyodbc.connect(**config)
        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            result.success = connection is not None
        except Exception as e:
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> Response:
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchall()
                if cur.description:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except Exception as e:
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        renderer = SqlalchemyRender(self.dialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        q = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        result = self.native_query(q)
        df = result.data_frame.rename(columns={'table_name': 'TABLE_NAME'})
        result.data_frame = df
        return result

    def get_columns(self, table_name: str) -> Response:
        q = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
        result = self.native_query(q)
        df = result.data_frame.rename(columns={'column_name': 'COLUMN_NAME', 'data_type': 'DATA_TYPE'})
        result.data_frame = df
        return result

connection_args = OrderedDict(
    driver={
        'type': ARG_TYPE.STR,
        'description': 'The driver to use for the Denodo connection.'
    },
    url={
        'type': ARG_TYPE.STR,
        'description': 'The URL for the Denodo connection.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user for the Denodo connection.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password for the Denodo connection.'
    }
)

connection_args_example = OrderedDict(
    driver='com.denodo.vdp.jdbc.Driver',
    url='jdbc:vdb://<hostname>:<port>/<database_name>',
    user='admin',
    password='password'
)
