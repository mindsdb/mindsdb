from typing import Optional

import pandas as pd
import sqlite3

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base_handler import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class SQLiteHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQLite statements.
    """

    name = 'sqlite'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'sqlite'
        self.connection_data = connection_data

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = sqlite3.connect(self.connection_data['db_file'])
        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler
        Returns:
            HandlerStatusResponse
        """

        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            result.success = connection.is_connected()
        except Exception as e:
            log.error(f'Error connecting to SQLite {self.connection_data["db_file"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result,
                        columns=[x[0] for x in cursor.description]
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.error(f'Error running query: {query} on {self.connection_data["db_file"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        cursor.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender('sqlite')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """ Return list of entities
         Return list of entities that will be accesible as tables.
         Returns:
             HandlerResponse: shoud have same columns as information_schema.tables
                 (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                 Column 'TABLE_NAME' is mandatory, other is optional.
         """
        pass

    def get_columns(self, table_name: str) -> StatusResponse:
        """ Returns a list of entity columns
          Args:
              table_name (str): name of one of tables returned by self.get_tables()
          Returns:
              HandlerResponse: shoud have same columns as information_schema.columns
                  (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                  Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                  recomended to define also 'DATA_TYPE': it should be one of
                  python data types (by default it str).
          """
        pass