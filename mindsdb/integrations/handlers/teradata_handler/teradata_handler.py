from typing import Any, Dict, Text

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from pandas import DataFrame
import teradatasql
from teradatasql import OperationalError
import teradatasqlalchemy.dialect as teradata_dialect

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class TeradataHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on Teradata.
    """

    name = 'teradata'

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Teradata database.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> teradatasql.TeradataConnection:
        """
        Establishes a connection to the Teradata database.

        Raises:
            ValueError: If the expected connection parameters are not provided.
            teradatasql.OperationalError: If an error occurs while connecting to the Teradata database.

        Returns:
            teradatasql.TeradataConnection: A connection object to the Teradata database.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['host', 'user', 'password']):
            raise ValueError('Required parameters (host, user, password) must be provided.')

        config = {
            'host': self.connection_data.get('host'),
            'user': self.connection_data.get('user'),
            'password': self.connection_data.get('password')
        }

        # Optional connection parameters.
        if 'database' in self.connection_data:
            config['database'] = self.connection_data.get('database')

        try:
            self.connection = teradatasql.connect(
                **config,
            )
            self.is_connected = True
            return self.connection
        except OperationalError as operational_error:
            logger.error(f'Error connecting to Teradata, {operational_error}!')
            raise
        except Exception as unknown_error:
            logger.error(f'Unknown error connecting to Teradata, {unknown_error}!')
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the Teradata database if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Teradata database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('SELECT 1 FROM (SELECT 1 AS "dual") AS "dual"')
            response.success = True
        except (OperationalError, ValueError) as known_error:
            logger.error(f'Connection check to Teradata failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Teradata failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the Teradata database and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                if not cur.description:
                    response = Response(RESPONSE_TYPE.OK)
                else:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                connection.commit()
            except OperationalError as operational_error:
                logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(operational_error)
                )
                connection.rollback()
            except Exception as unknown_error:
                logger.error(f'Unknown error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(unknown_error)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Teradata database and retrieves the data (if any).

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(teradata_dialect.TeradataDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables in the Teradata database.

        Returns:
            Response: A response object containing a list of tables in the Teradata database.
        """
        query = f"""
            SELECT
                TableName AS table_name,
                TableKind AS table_type
            FROM DBC.TablesV
            WHERE DatabaseName = '{self.connection_data.get('database') if self.connection_data.get('database') else self.connection_data.get('user')}'
            AND (TableKind = 'T'
                OR TableKind = 'O'
                OR TableKind = 'Q'
                OR TableKind = 'V')
        """
        result = self.native_query(query)

        df = result.data_frame
        df['table_type'] = df['table_type'].apply(lambda x: 'VIEW' if x == 'V' else 'BASE TABLE')

        result.data_frame = df
        return result

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table in the Teradata database.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        query = f"""
            SELECT ColumnName AS "Field",
                   ColumnType AS "Type"
            FROM DBC.ColumnsV
            WHERE DatabaseName = '{self.connection_data.get('database') if self.connection_data.get('database') else self.connection_data.get('user')}'
            AND TableName = '{table_name}'
        """

        return self.native_query(query)
