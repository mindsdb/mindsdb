import pymssql
from pymssql import OperationalError
import pandas as pd

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class SqlServerHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Microsoft SQL Server statements.
    """
    name = 'mssql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs.get('connection_data')
        self.dialect = 'mssql'
        self.database = self.connection_args.get('database')
        self.renderer = SqlalchemyRender('mssql')

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Establishes a connection to a Microsoft SQL Server database.

        Raises:
            pymssql._mssql.OperationalError: If an error occurs while connecting to the Microsoft SQL Server database.

        Returns:
            pymssql.Connection: A connection object to the Microsoft SQL Server database.
        """

        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters
        if not all(key in self.connection_args for key in ['host', 'user', 'password', 'database']):
            raise ValueError('Required parameters (host, user, password, database) must be provided.')

        config = {
            'host': self.connection_args.get('host'),
            'user': self.connection_args.get('user'),
            'password': self.connection_args.get('password'),
            'database': self.connection_args.get('database')
        }

        # Optional connection parameters
        if 'port' in self.connection_args:
            config['port'] = self.connection_args.get('port')

        if 'server' in self.connection_args:
            config['server'] = self.connection_args.get('server')

        try:
            self.connection = pymssql.connect(**config)
            self.is_connected = True
            return self.connection
        except OperationalError as e:
            logger.error(f'Error connecting to Microsoft SQL Server {self.database}, {e}!')
            self.is_connected = False
            raise

    def disconnect(self):
        """
        Closes the connection to the Microsoft SQL Server database if it's currently open.
        """

        if not self.is_connected:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Microsoft SQL Server database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                # Execute a simple query to test the connection
                cur.execute('select 1;')
            response.success = True
        except OperationalError as e:
            logger.error(f'Error connecting to Microsoft SQL Server {self.database}, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()
        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query on the Microsoft SQL Server database and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor(as_dict=True) as cur:
            try:
                cur.execute(query)
                if cur.description:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.database}, {e}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """

        query_str = self.renderer.get_string(query, with_failback=True)
        logger.debug(f"Executing SQL query: {query_str}")
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables and views in the current schema of the Microsoft SQL Server database.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """

        query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE in ('BASE TABLE', 'VIEW');
        """
        return self.native_query(query)

    def get_columns(self, table_name) -> Response:
        """
        Retrieves column details for a specified table in the Microsoft SQL Server database.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """

        query = f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        return self.native_query(query)
