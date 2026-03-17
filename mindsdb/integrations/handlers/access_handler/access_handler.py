from typing import Optional
import platform

import pandas as pd

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

try:
    import pyodbc
    from sqlalchemy_access.base import AccessDialect

    IMPORT_ERROR = None
except ImportError as e:
    pyodbc = None
    AccessDialect = None
    IMPORT_ERROR = e

logger = log.getLogger(__name__)


class AccessHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Microsoft Access statements.
    """

    name = "access"

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
        self.dialect = "access"
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Set up the connection required by the handler.
        Returns:
            pyodbc.Connection: A connection object to the Access database.
        """
        if self.is_connected is True:
            return self.connection

        if IMPORT_ERROR is not None:
            raise RuntimeError(
                f"Microsoft Access handler requires pyodbc and sqlalchemy-access packages. "
                f"Install them with: pip install pyodbc sqlalchemy-access. Error: {IMPORT_ERROR}"
            )

        if platform.system() != "Windows":
            raise RuntimeError(
                "Microsoft Access handler is only supported on Windows. "
                "The Microsoft Access ODBC driver is not available on other operating systems."
            )

        self.connection = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + self.connection_data["db_file"]
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
            logger.error(f"Error connecting to Microsoft Access database {self.connection_data['db_file']}, {e}!")
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
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame.from_records(result, columns=[x[0] for x in cursor.description]),
                    )

                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except Exception as e:
                logger.error(f"Error running query: {query} on {self.connection_data['db_file']}!")
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
        if IMPORT_ERROR is not None:
            raise RuntimeError(
                f"Microsoft Access handler requires pyodbc and sqlalchemy-access packages. "
                f"Install them with: pip install pyodbc sqlalchemy-access. Error: {IMPORT_ERROR}"
            )

        renderer = SqlalchemyRender(AccessDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """
        connection = self.connect()
        with connection.cursor() as cursor:
            df = pd.DataFrame([table.table_name for table in cursor.tables(tableType="Table")], columns=["table_name"])

        response = Response(RESPONSE_TYPE.TABLE, df)

        return response

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """
        connection = self.connect()
        with connection.cursor() as cursor:
            df = pd.DataFrame(
                [(column.column_name, column.type_name) for column in cursor.columns(table=table_name)],
                columns=["column_name", "data_type"],
            )

        response = Response(RESPONSE_TYPE.TABLE, df)

        return response
