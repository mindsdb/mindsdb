from collections import OrderedDict

import psycopg
from psycopg.postgres import types
from psycopg.pq import ExecStatus
from pandas import DataFrame

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
import mindsdb.utilities.profiler as profiler

logger = log.getLogger(__name__)


class PostgresHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the PostgreSQL statements.
    """
    name = 'postgres'

    @profiler.profile('init_pg_handler')
    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs.get('connection_data')
        self.dialect = 'postgresql'
        self.database = self.connection_args.get('database')
        self.renderer = SqlalchemyRender('postgres')

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    @profiler.profile()
    def connect(self):
        """
        Establishes a connection to a PostgreSQL database.

        Raises:
            psycopg.Error: If an error occurs while connecting to the PostgreSQL database.

        Returns:
            psycopg.Connection: A connection object to the PostgreSQL database.
        """
        if self.is_connected:
            return self.connection

        config = {
            'host': self.connection_args.get('host'),
            'port': self.connection_args.get('port'),
            'user': self.connection_args.get('user'),
            'password': self.connection_args.get('password'),
            'dbname': self.connection_args.get('database')
        }

        if self.connection_args.get('sslmode'):
            config['sslmode'] = self.connection_args.get('sslmode')

        # If schema is not provided set public as default one
        if self.connection_args.get('schema'):
            config['options'] = f'-c search_path={self.connection_args.get("schema")},public'

        try:
            self.connection = psycopg.connect(**config, connect_timeout=10)
            self.is_connected = True
            return self.connection
        except psycopg.Error as e:
            logger.error(f'Error connecting to PostgreSQL {self.database}, {e}!')
            self.is_connected = False
            raise

    def disconnect(self):
        """
        Closes the connection to the PostgreSQL database if it's currently open.
        """
        if not self.is_connected:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the PostgreSQL database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                # Execute a simple query to test the connection
                cur.execute('select 1;')
            response.success = True
        except psycopg.Error as e:
            logger.error(f'Error connecting to PostgreSQL {self.database}, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()
        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def _cast_dtypes(self, df: DataFrame, description: list) -> None:
        """
        Cast df dtypes basing on postgres types
            Note:
                Date types casting is not provided because of there is no issues (so far).
                By default pandas will cast postgres date types to:
                 - date -> object
                 - time -> object
                 - timetz -> object
                 - timestamp -> datetime64[ns]
                 - timestamptz -> datetime64[ns, {tz}]

            Args:
                df (DataFrame)
                description (list): psycopg cursor description
        """
        types_map = {
            'int2': 'int16',
            'int4': 'int32',
            'int8': 'int64',
            'numeric': 'float64',
            'float4': 'float32',
            'float8': 'float64'
        }
        for column_index, _ in enumerate(df.columns):
            col = df.iloc[:, column_index]  # column names could be duplicated
            if str(col.dtype) == 'object':
                pg_type = types.get(description[column_index].type_code)
                if pg_type is not None and pg_type.name in types_map:
                    col = col.fillna(0)
                    try:
                        df.iloc[:, column_index] = col.astype(types_map[pg_type.name])
                    except ValueError as e:
                        logger.error(f'Error casting column {col.name} to {types_map[pg_type.name]}: {e}')

    @profiler.profile()
    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query on the PostgreSQL database and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = not self.is_connected

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                if ExecStatus(cur.pgresult.status) == ExecStatus.COMMAND_OK:
                    response = Response(RESPONSE_TYPE.OK)
                else:
                    result = cur.fetchall()
                    df = DataFrame(
                        result,
                        columns=[x.name for x in cur.description]
                    )
                    self._cast_dtypes(df, cur.description)
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        df
                    )
                connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.database}, {e}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close:
            self.disconnect()
        return response

    @profiler.profile()
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
        Retrieves a list of all non-system tables and views in the current schema of the PostgreSQL database.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        query = """
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('information_schema', 'pg_catalog')
                and table_type in ('BASE TABLE', 'VIEW')
                and table_schema = current_schema()
        """
        return self.native_query(query)

    def get_columns(self, table_name: str) -> Response:
        """
        Retrieves column details for a specified table in the PostgreSQL database.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """

        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

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


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the PostgreSQL server.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the PostgreSQL server.',
        'required': True,
        'label': 'Password'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the PostgreSQL server.',
        'required': True,
        'label': 'Database'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the PostgreSQL server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the PostgreSQL server. Must be an integer.',
        'required': True,
        'label': 'Port'
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'The schema in which objects are searched first.',
        'required': False,
        'label': 'Schema'
    },
    sslmode={
        'type': ARG_TYPE.STR,
        'description': 'sslmode that will be used for connection.',
        'required': False,
        'label': 'sslmode'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5432,
    user='root',
    schema='public',
    password='password',
    database='database'
)
