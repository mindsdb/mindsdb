from typing import Text, Dict, Optional

import oracledb
import pandas as pd
from oracledb import connect, Connection, DatabaseError
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


oracledb.defaults.fetch_lobs = False  # Return LOBs directly as strings or bytes.
logger = log.getLogger(__name__)


def _map_type(internal_type_name: str) -> MYSQL_DATA_TYPE:
    """ Map Oracle types to MySQL types.
        List of types: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html

    Args:
        internal_type_name (str): The name of the Oracle type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type that corresponds to the Oracle type.
    """
    internal_type_name = internal_type_name.upper()
    types_map = {
        (
            'VARCHAR2', 'NVARCHAR2', 'CHARACTER VARYING', 'CHAR VARYING', 'NATIONAL CHARACTER', 'NATIONAL CHAR',
            'VARCHAR', 'NCHAR', 'NATIONAL CHARACTER VARYING', 'NATIONAL CHAR VARYING', 'NCHAR VARYING', 'LONG VARCHAR'
        ): MYSQL_DATA_TYPE.VARCHAR,
        ('INTEGER', 'INT'): MYSQL_DATA_TYPE.INT,
        ('SMALLINT',): MYSQL_DATA_TYPE.SMALLINT,
        ('NUMBER', 'DECIMAL'): MYSQL_DATA_TYPE.DECIMAL,
        ('FLOAT', 'BINARY_FLOAT', 'REAL'): MYSQL_DATA_TYPE.FLOAT,
        ('BINARY_DOUBLE',): MYSQL_DATA_TYPE.DOUBLE,
        ('LONG',): MYSQL_DATA_TYPE.BIGINT,
        ('DATE',): MYSQL_DATA_TYPE.DATE,
        ('HOUR', 'MINUTE', 'SECOND', 'TIMEZONE_HOUR', 'TIMEZONE_MINUTE'): MYSQL_DATA_TYPE.SMALLINT,
        ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE'): MYSQL_DATA_TYPE.TIMESTAMP,
        ('RAW', 'LONG RAW', 'BLOB', 'BFILE'): MYSQL_DATA_TYPE.BINARY,
        ('ROWID', 'UROWID'): MYSQL_DATA_TYPE.TEXT,
        ('CHAR', 'NCHAR', 'CLOB', 'NCLOB', 'CHARACTER'): MYSQL_DATA_TYPE.CHAR,
    }

    for db_types_list, mysql_data_type in types_map.items():
        if internal_type_name in db_types_list:
            return mysql_data_type

    logger.warning(f"Oracle handler type mapping: unknown type: {internal_type_name}, use VARCHAR as fallback.")
    return MYSQL_DATA_TYPE.VARCHAR


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

            if 'session_variables' in self.connection_data:
                with connection.cursor() as cur:
                    for key, value in self.connection_data['session_variables'].items():
                        cur.execute(f"ALTER SESSION SET {key} = {repr(value)}")

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
                if cur.description is None:
                    response = Response(RESPONSE_TYPE.OK, affected_rows=cur.rowcount)
                else:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[row[0] for row in cur.description],
                        ),
                    )

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
                column_name AS field,
                data_type AS type
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{table_name}'
        """
        result = self.native_query(query)
        if result.resp_type is RESPONSE_TYPE.TABLE:
            result.data_frame.columns = [name.lower() for name in result.data_frame.columns]
            result.data_frame['mysql_data_type'] = result.data_frame['type'].apply(_map_type)
        return result
