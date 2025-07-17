from typing import Optional

import pandas as pd
import libsql_experimental as libsql

from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)


logger = log.getLogger(__name__)


class LibSQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the LibSQL statements.
    """

    name = "libsql"

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
        self.dialect = "libsql"
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

        args = self.connection_data
        # sync_url and auth_token are optional
        # sync_url is used to sync the local database from the remote database
        # auth_token is used as the authentication token for the remote database
        if args.get("sync_url"):
            self.connection = libsql.connect(
                database=args["database"],
                sync_url=args["sync_url"],
                auth_token=args["auth_token"],
            )
        else:
            self.connection = libsql.connect(database=args["database"])

        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection = None
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
            logger.error(
                f'Error connecting to SQLite {self.connection_data["database"]}, {e}!'
            )
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

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
                        result, columns=[x[0] for x in cursor.description]
                    ),
                )
            else:
                connection.commit()
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(
                f'Error running query: {query} on {self.connection_data["database"]}!'
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        return self.native_query(query)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        query = "SELECT name from sqlite_master where type= 'table';"
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: "table_name"})
        return result

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"PRAGMA table_info([{table_name}]);"
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(
            columns={"name": "column_name", "type": "data_type"}
        )
        return result
