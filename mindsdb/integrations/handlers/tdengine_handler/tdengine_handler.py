from collections import OrderedDict
from typing import Optional
import pandas as pd
import taosrest as td
from taosrest import sqlalchemy as SA

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


log = get_log()


class TDEngineHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the TDEngine statements.
    """

    name = 'tdengine'

    def __init__(self, name, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        
        self.parser = parse_sql
        self.dialect = 'tdengine'
        self.kwargs = kwargs
        self.connection_data = connection_data

        self.connection = None
        self.is_connected = False


    def connect(self):
        if self.is_connected is True:
            return self.connection

        config = {
            'url': self.connection_data.get('url', "http://localhost:6041"),
            'token': self.connection_data.get('token'),
            'user': self.connection_data.get('user','root'),
            'password': self.connection_data.get('password','taosdata'),
            'database': self.connection_data.get('database')
        }

        connection = td.connect(**config)
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
            log.error(f'Error connecting to TDEngine {self.connection_data["database"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in TDEngine
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        cur = connection.cursor() 
        try:
                cur.execute(query)
                
                if cur.rowcount != 0:
                    result = cur.fetchall()
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
                log.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                # connection.rollback()
        cur.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender(SA.TaosRestDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in TDEngine
        """
        q = 'SHOW TABLES;'

         
        return self.native_query(q)

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f'DESCRIBE {table_name};'

        
        return self.native_query(q)


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the TDEngine server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the TDEngine server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the TDEngine server.'
    },
    url={
        'type': ARG_TYPE.STR,
        'description': 'The url of the TDEngine server Instance. '
    },
    token={
        'type': ARG_TYPE.INT,
        'description': 'Unique Token to COnnect TDEngine'
    },


)

connection_args_example = OrderedDict(
    url='127.0.0.1:6041',
    token='<token comes here>',
    user='root',
    password='taosdata',
    database='test'
)
