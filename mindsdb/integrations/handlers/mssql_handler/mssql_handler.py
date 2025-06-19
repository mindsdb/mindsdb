from typing import Any
import datetime

import pymssql
from pymssql import OperationalError
import pandas as pd
from pandas.api import types as pd_types

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
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


logger = log.getLogger(__name__)


def _map_type(mssql_type_text: str) -> MYSQL_DATA_TYPE:
    """ Map MSSQL text types names to MySQL types as enum.

    Args:
        mssql_type_text (str): The name of the MSSQL type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type enum that corresponds to the MSSQL text type name.
    """
    internal_type_name = mssql_type_text.lower()
    types_map = {
        ('tinyint', 'smallint', 'int', 'bigint'): MYSQL_DATA_TYPE.INT,
        ('bit',): MYSQL_DATA_TYPE.BOOL,
        ('money', 'smallmoney', 'float', 'real'): MYSQL_DATA_TYPE.FLOAT,
        ('decimal', 'numeric'): MYSQL_DATA_TYPE.DECIMAL,
        ('date',): MYSQL_DATA_TYPE.DATE,
        ('time',): MYSQL_DATA_TYPE.TIME,
        ('datetime2', 'datetimeoffset', 'datetime', 'smalldatetime'): MYSQL_DATA_TYPE.DATETIME,
        ('varchar', 'nvarchar'): MYSQL_DATA_TYPE.VARCHAR,
        ('char', 'text', 'nchar', 'ntext'): MYSQL_DATA_TYPE.TEXT,
        ('binary', 'varbinary', 'image'): MYSQL_DATA_TYPE.BINARY
    }

    for db_types_list, mysql_data_type in types_map.items():
        if internal_type_name in db_types_list:
            return mysql_data_type

    logger.debug(f"MSSQL handler type mapping: unknown type: {internal_type_name}, use VARCHAR as fallback.")
    return MYSQL_DATA_TYPE.VARCHAR


def _make_table_response(result: list[dict[str, Any]], cursor: pymssql.Cursor) -> Response:
    """Build response from result and cursor.

    Args:
        result (list[dict[str, Any]]): result of the query.
        cursor (pymssql.Cursor): cursor object.

    Returns:
        Response: response object.
    """
    description: list[tuple[Any]] = cursor.description
    mysql_types: list[MYSQL_DATA_TYPE] = []

    data_frame = pd.DataFrame(
        result,
        columns=[x[0] for x in cursor.description]
    )

    for column in description:
        column_name = column[0]
        column_type = column[1]
        column_dtype = data_frame[column_name].dtype
        match column_type:
            case pymssql.NUMBER:
                if pd_types.is_integer_dtype(column_dtype):
                    mysql_types.append(MYSQL_DATA_TYPE.INT)
                elif pd_types.is_float_dtype(column_dtype):
                    mysql_types.append(MYSQL_DATA_TYPE.FLOAT)
                elif pd_types.is_bool_dtype(column_dtype):
                    # it is 'bit' type
                    mysql_types.append(MYSQL_DATA_TYPE.TINYINT)
                else:
                    mysql_types.append(MYSQL_DATA_TYPE.DOUBLE)
            case pymssql.DECIMAL:
                mysql_types.append(MYSQL_DATA_TYPE.DECIMAL)
            case pymssql.STRING:
                mysql_types.append(MYSQL_DATA_TYPE.TEXT)
            case pymssql.DATETIME:
                mysql_types.append(MYSQL_DATA_TYPE.DATETIME)
            case pymssql.BINARY:
                # DATE and TIME types returned as 'BINARY' type, and dataframe type is 'object', so it is not possible
                # to infer correct mysql type for them
                if pd_types.is_datetime64_any_dtype(column_dtype):
                    # pymssql return datetimes as 'binary' type
                    # if timezone is present, then it is datetime.timezone
                    series = data_frame[column_name]
                    if (
                        series.dt.tz is not None
                        and isinstance(series.dt.tz, datetime.timezone)
                        and series.dt.tz != datetime.timezone.utc
                    ):
                        series = series.dt.tz_convert('UTC')
                        data_frame[column_name] = series.dt.tz_localize(None)
                    mysql_types.append(MYSQL_DATA_TYPE.DATETIME)
                else:
                    mysql_types.append(MYSQL_DATA_TYPE.BINARY)
            case _:
                logger.warning(f"Unknown type: {column_type}, use TEXT as fallback.")
                mysql_types.append(MYSQL_DATA_TYPE.TEXT)

    return Response(
        RESPONSE_TYPE.TABLE,
        data_frame=data_frame,
        mysql_types=mysql_types
    )


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
                    response = _make_table_response(result, cur)
                else:
                    response = Response(RESPONSE_TYPE.OK, affected_rows=cur.rowcount)
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
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                COLUMN_DEFAULT,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                COLLATION_NAME
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        result = self.native_query(query)
        result.to_columns_table_response(map_type_fn=_map_type)
        return result
