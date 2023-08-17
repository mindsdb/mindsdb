from collections import OrderedDict

import psycopg
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
        if self.is_connected is True:
            self.disconnect()

    @profiler.profile()
    def connect(self):
        """
        Handles the connection to a PostgreSQL database instance.
        """
        if self.is_connected is True:
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

        if self.connection_args.get('schema'):
            config['options'] = f'-c search_path={self.connection_args.get("schema")},public'

        connection = psycopg.connect(**config, connect_timeout=10)

        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('select 1;')
            response.success = True
        except psycopg.Error as e:
            logger.error(f'Error connecting to PostgreSQL {self.database}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    @profiler.profile()
    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in PostgreSQL
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                if ExecStatus(cur.pgresult.status) == ExecStatus.COMMAND_OK:
                    response = Response(RESPONSE_TYPE.OK)
                else:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        DataFrame(
                            result,
                            columns=[x.name for x in cur.description]
                        )
                    )
                connection.commit()
            except Exception as e:
                logger.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    @profiler.profile()
    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tables in PostgreSQL without the system tables information_schema and pg_catalog
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
        """
        return self.native_query(query)

    def get_columns(self, table_name: str) -> Response:
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
    password='password',
    database='database'
)
