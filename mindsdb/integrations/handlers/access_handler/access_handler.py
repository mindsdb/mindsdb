from typing import Optional

import pandas as pd
import pyodbc

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from sqlalchemy_access.base import AccessDialect
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class AccessHandler(DatabaseHandler):
    """
    This handler manages connection and execution of Microsoft Access statements.
    """

    name = 'access'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): Name of the handler instance.
            connection_data (dict): Parameters for connecting to the database.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'access'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return StatusResponse(success=True)

        try:
            self.connection = pyodbc.connect(
                r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.connection_data['db_file']
            )
            self.is_connected = True
            logger.info(f"Connected to Access database: {self.connection_data['db_file']}")
        except Exception as e:
            logger.error(f"Failed to connect to Access database: {e}")
            return StatusResponse(success=False, error_message=str(e))

        return StatusResponse(success=True)

    def disconnect(self):
        """
        Close any existing connections.
        """
        if not self.is_connected:
            return

        try:
            self.connection.close()
            logger.info("Disconnected from Access database.")
        except Exception as e:
            logger.error(f"Error disconnecting from database: {e}")
        finally:
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return StatusResponse(success=True)

        logger.info("Checking connection to Access database.")
        return self.connect()

    def native_query(self, query: str) -> Response:
        """
        Execute a raw query.
        Args:
            query (str): Query in native format.
        Returns:
            HandlerResponse
        """
        if not self.is_connected:
            connection_status = self.connect()
            if not connection_status.success:
                return Response(RESPONSE_TYPE.ERROR, error_message=connection_status.error_message)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    df = pd.DataFrame.from_records(result, columns=[x[0] for x in cursor.description])
                    logger.info(f"Query executed successfully: {query}")
                    return Response(RESPONSE_TYPE.TABLE, data_frame=df)
                else:
                    self.connection.commit()
                    logger.info(f"Query executed successfully with no result set: {query}")
                    return Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Error running query: {query}, {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def query(self, query: ASTNode) -> Response:
        """
        Execute a query provided as an AST (abstract syntax tree).
        Args:
            query (ASTNode): SQL query represented as AST. Can be SELECT, INSERT, DELETE, etc.
        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender(AccessDialect)
        query_str = renderer.get_string(query, with_failback=True)
        logger.debug(f"Rendering AST to SQL: {query_str}")
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Return a list of tables in the database.
        Returns:
            HandlerResponse
        """
        if not self.is_connected:
            connection_status = self.connect()
            if not connection_status.success:
                return Response(RESPONSE_TYPE.ERROR, error_message=connection_status.error_message)

        try:
            with self.connection.cursor() as cursor:
                tables = [table.table_name for table in cursor.tables(tableType='TABLE')]
                df = pd.DataFrame(tables, columns=['table_name'])
                logger.info("Retrieved list of tables.")
                return Response(RESPONSE_TYPE.TABLE, data_frame=df)
        except Exception as e:
            logger.error(f"Error retrieving tables: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def get_columns(self, table_name: str) -> Response:
        """
        Return a list of columns for a given table.
        Args:
            table_name (str): Name of the table.
        Returns:
            HandlerResponse
        """
        if not self.is_connected:
            connection_status = self.connect()
            if not connection_status.success:
                return Response(RESPONSE_TYPE.ERROR, error_message=connection_status.error_message)

        try:
            with self.connection.cursor() as cursor:
                columns = [(column.column_name, column.type_name) for column in cursor.columns(table=table_name)]
                df = pd.DataFrame(columns, columns=['column_name', 'data_type'])
                logger.info(f"Retrieved columns for table: {table_name}")
                return Response(RESPONSE_TYPE.TABLE, data_frame=df)
        except Exception as e:
            logger.error(f"Error retrieving columns for table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))