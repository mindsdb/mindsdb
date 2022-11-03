import psycopg
from psycopg.pq import ExecStatus
from pandas import DataFrame

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class PostgresHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the PostgreSQL statements.
    """
    name = 'postgres'

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

    def connect(self):
        """
        Handles the connection to a PostgreSQL database insance.
        """
        if self.is_connected is True:
            return self.connection

        # TODO: Check psycopg_pool
        if self.connection_args.get('dbname') is None:
            self.connection_args['dbname'] = self.database
        args = self.connection_args.copy()

        for key in ['type', 'publish', 'test', 'date_last_update', 'integrations_name', 'database_name', 'id', 'database']:
            if key in args:
                del args[key]
        connection = psycopg.connect(**args, connect_timeout=10)

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
            log.logger.error(f'Error connecting to PostgreSQL {self.database}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

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
                log.logger.error(f'Error running query: {query} on {self.database}!')
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
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tabels in PostgreSQL without the system tables information_schema and pg_catalog
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

    def get_columns(self, table_name):
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
