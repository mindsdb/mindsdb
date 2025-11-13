import pandas as pd
from typing import Optional
import psycopg2 as dbdriver
from psycopg2 import OperationalError, InterfaceError, ProgrammingError

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

logger = log.getLogger(__name__)


class DenodoHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Denodo statements.
    """

    name = "denodo"

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "mysql"
        self.connection_data = kwargs.get("connection_data", {})
        self.database = self.connection_data.get("database")

        self.connection = None

    def connect(self) -> Optional[dbdriver.extensions.connection]:
        """
        Connect to the Denodo database using the connection data provided.

        Returns:
            Optional[dbdriver.extensions.connection]: A connection object if successful, None otherwise.
        """
        if self.connection is not None:
            return self.connection

        try:
            self.connection = dbdriver.connect(
                host=self.connection_data.get("host"),
                port=self.connection_data.get("port"),
                user=self.connection_data.get("user"),
                password=self.connection_data.get("password"),
                database=self.connection_data.get("database"),
            )
            return self.connection
        except (OperationalError, InterfaceError) as e:
            logger.error(f"Error connecting to Denodo: {str(e)}")
            raise ConnectionError(f"Failed to connect to Denodo: {str(e)}")

    def disconnect(self) -> None:
        """
        Safely close the database connection.
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def _validate_connection(self) -> None:
        """
        Check if the connection is still active and reconnect if necessary.
        """
        if not self.connection:
            self.connect()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except (OperationalError, InterfaceError):
            self.connect()

    def check_connection(self) -> StatusResponse:
        """
        Check if the connection is still active.

        Returns:
            StatusResponse: A response object containing the status of the connection.
        """
        try:
            self._validate_connection()
            return StatusResponse(True)
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return StatusResponse(False, str(e))

    def native_query(self, query: str) -> Response:
        """
        Executes a VQL query on the Denodo database and returns the result.

        Args:
            query (str): The VQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        self._validate_connection()

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute(query)
                if cur.description is not None:
                    columns = [desc[0] for desc in cur.description]
                    result = cur.fetchall()
                    response = Response(
                        resp_type=RESPONSE_TYPE.TABLE,
                        query=query,
                        data_frame=pd.DataFrame(result, columns=columns),
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)

        except (OperationalError, InterfaceError, ProgrammingError) as e:
            logger.error(f"Error running query: {query} on {self.database}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Execute a SQL query and return results.
        """
        renderer = SqlalchemyRender(self.dialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get all tables in current schema.
        """
        query = "SELECT name FROM GET_VIEWS();"
        result = self.native_query(query)
        df = result.data_frame.rename(columns={"name": "TABLE_NAME"})
        result.data_frame = df
        return result

    def get_columns(self, table_name: str) -> Response:
        """
        Get columns for specified table using parameterized query.
        """
        query = f"CALL GET_VIEW_COLUMNS('{self.database}', '{table_name}');"
        result = self.native_query(query)
        df = result.data_frame.rename(
            columns={"column_name": "COLUMN_NAME", "data_type": "DATA_TYPE"}
        )
        result.data_frame = df
        return result
