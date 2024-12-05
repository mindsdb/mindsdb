from typing import Any, Dict, Text

from hdbcli import dbapi
from hdbcli.dbapi import Error, ProgrammingError
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from pandas import DataFrame
import sqlalchemy_hana.dialect as hana_dialect

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class HanaHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on SAP HANA.
    """

    name = 'hana'

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the SAP HANA database.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> dbapi.Connection:
        """
        Establishes a connection to the SAP HANA database.

        Raises:
            ValueError: If the expected connection parameters are not provided.
            hdbcli.dbapi.Error: If an error occurs while connecting to the SAP HANA database.

        Returns:
            hdbcli.dbapi.Connection: A connection object to the SAP HANA database.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['address', 'port', 'user', 'password']):
            raise ValueError('Required parameters (address, port, user, password) must be provided.')

        config = {
            'address': self.connection_data['address'],
            'port': self.connection_data['port'],
            'user': self.connection_data['user'],
            'password': self.connection_data['password'],
        }

        # Optional connection parameters.
        if 'database' in self.connection_data:
            config['databaseName'] = self.connection_data['database']

        if 'schema' in self.connection_data:
            config['currentSchema'] = self.connection_data['schema']

        if 'encrypt' in self.connection_data:
            config['encrypt'] = self.connection_data['encrypt']

        try:
            self.connection = dbapi.connect(
                **config
            )
            self.is_connected = True
            return self.connection
        except Error as known_error:
            logger.error(f'Error connecting to SAP HANA, {known_error}!')
            raise
        except Exception as unknown_error:
            logger.error(f'Unknown error connecting to Teradata, {unknown_error}!')
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the SAP HANA database if it's currently open.
        """
        if self.is_connected is True:
            self.connection.close()
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the SAP HANA database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('SELECT 1 FROM SYS.DUMMY')
            response.success = True
        except (Error, ProgrammingError, ValueError) as known_error:
            logger.error(f'Connection check to SAP HANA failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to SAP HANA failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the SAP HANA database and returns the result.

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
            except ProgrammingError as programming_error:
                logger.error(f'Error running query: {query} on {self.address}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(programming_error)
                )
                connection.rollback()
            except Exception as unknown_error:
                logger.error(f'Unknown error running query: {query} on {self.address}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(unknown_error)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the SAP HANA database and retrieves the data (if any).

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(hana_dialect.HANAHDBCLIDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables in the SAP HANA database.

        Returns:
            Response: A response object containing a list of tables in the SAP HANA database.
        """
        query = """
            SELECT SCHEMA_NAME,
                   TABLE_NAME,
                   'BASE TABLE' AS TABLE_TYPE
            FROM
                SYS.TABLES
            WHERE IS_SYSTEM_TABLE = 'FALSE'
              AND IS_USER_DEFINED_TYPE = 'FALSE'
              AND IS_TEMPORARY = 'FALSE'

            UNION

            SELECT SCHEMA_NAME,
                   VIEW_NAME AS TABLE_NAME,
                   'VIEW' AS TABLE_TYPE
            FROM
                SYS.VIEWS
            WHERE SCHEMA_NAME <> 'SYS'
              AND SCHEMA_NAME NOT LIKE '_SYS%'
        """
        return self.native_query(query)

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table in the SAP HANA database.

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
            SELECT COLUMN_NAME AS Field,
                DATA_TYPE_NAME AS Type
            FROM SYS.TABLE_COLUMNS
            WHERE TABLE_NAME = '{table_name}'

            UNION ALL

            SELECT COLUMN_NAME AS Field,
                DATA_TYPE_NAME AS Type
            FROM SYS.VIEW_COLUMNS
            WHERE VIEW_NAME = '{table_name}'
        """
        return self.native_query(query)
