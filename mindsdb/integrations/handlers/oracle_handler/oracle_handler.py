from typing import Text, Dict, Optional

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
import oracledb
from oracledb import connect, Connection, DatabaseError
import pandas as pd

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


oracledb.defaults.fetch_lobs = False  # Return LOBs directly as strings or bytes.
logger = log.getLogger(__name__)


class OracleHandler(DatabaseHandler):
    """
    This handler handles connection and execution of SQL queries on Oracle.
    """

    name = "oracle"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to Amazon DynamoDB.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self) -> Connection:
        """
        Establishes a connection to the Oracle database.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            oracledb.Connection: A connection object to the Oracle database.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['user', 'password']):
            raise ValueError('Required parameters (user, password) must be provided.')

        config = {
            'user': self.connection_data['user'],
            'password': self.connection_data['password'],
        }

        # If 'dsn' is given, use it. Otherwise, use the individual connection parameters.
        if 'dsn' in self.connection_data:
            config['dsn'] = self.connection_data['dsn']

        else:
            if 'host' not in self.connection_data and not any(key in self.connection_data for key in ['sid', 'service_name']):
                raise ValueError('Required parameter host and either sid or service_name must be provided. Alternatively, dsn can be provided.')

            config['host'] = self.connection_data.get('host')

            # Optional connection parameters when 'dsn' is not given.
            optional_parameters = ['port', 'sid', 'service_name']
            for parameter in optional_parameters:
                if parameter in self.connection_data:
                    config[parameter] = self.connection_data[parameter]

        # Other optional connection parameters.
        if 'disable_oob' in self.connection_data:
            config['disable_oob'] = self.connection_data['disable_oob']

        if 'auth_mode' in self.connection_data:
            mode_name = 'AUTH_MODE_' + self.connection_data['auth_mode'].upper()
            if not hasattr(oracledb, mode_name):
                raise ValueError(f'Unknown auth mode: {mode_name}')
            config['mode'] = getattr(oracledb, mode_name)

        try:
            connection = connect(
                **config,
            )
        except DatabaseError as database_error:
            logger.error(f'Error connecting to Oracle, {database_error}!')
            raise

        except Exception as unknown_error:
            logger.error(f'Unknown error when connecting to Elasticsearch: {unknown_error}')
            raise

        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        """
        Closes the connection to the Oracle database if it's currently open.
        """
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Oracle database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            con = self.connect()
            con.ping()
            response.success = True
        except (ValueError, DatabaseError) as known_error:
            logger.error(f'Connection check to Oracle failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Oracle failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a SQL query on the Oracle database and returns the result.

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
                result = cur.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[row[0] for row in cur.description],
                        ),
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)

                connection.commit()
            except DatabaseError as database_error:
                logger.error(f"Error running query: {query} on Oracle, {database_error}!")
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(database_error),
                )
                connection.rollback()

            except Exception as unknown_error:
                logger.error(f"Unknwon error running query: {query} on Oracle, {unknown_error}!")
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(unknown_error),
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
        renderer = SqlalchemyRender("oracle")
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables and views in the current schema of the Oracle database.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        # TODO: This query does not seem to be correct.
        query = """
            SELECT table_name
            FROM user_tables
            ORDER BY 1
        """
        return self.native_query(query)

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table in the Oracle database.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """
        query = f"""
            SELECT
                column_name,
                data_type
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{table_name}'
        """
        result = self.native_query(query)
        return result
