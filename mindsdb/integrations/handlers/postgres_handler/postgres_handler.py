import time
import json
from typing import Optional

import pandas as pd
import psycopg
from psycopg.postgres import types
from psycopg.pq import ExecStatus
from pandas import DataFrame

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
import mindsdb.utilities.profiler as profiler

logger = log.getLogger(__name__)

SUBSCRIBE_SLEEP_INTERVAL = 1


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
        self.thread_safe = True

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    def _make_connection_args(self):
        config = {
            'host': self.connection_args.get('host'),
            'port': self.connection_args.get('port'),
            'user': self.connection_args.get('user'),
            'password': self.connection_args.get('password'),
            'dbname': self.connection_args.get('database')
        }

        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        connection_parameters = self.connection_args.get('connection_parameters')
        if isinstance(connection_parameters, dict) is False:
            connection_parameters = {}
        if 'connect_timeout' not in connection_parameters:
            connection_parameters['connect_timeout'] = 10
        config.update(connection_parameters)

        if self.connection_args.get('sslmode'):
            config['sslmode'] = self.connection_args.get('sslmode')

        if self.connection_args.get('autocommit'):
            config['autocommit'] = self.connection_args.get('autocommit')

        # If schema is not provided set public as default one
        if self.connection_args.get('schema'):
            config['options'] = f'-c search_path={self.connection_args.get("schema")},public'
        return config

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

        config = self._make_connection_args()
        try:
            self.connection = psycopg.connect(**config)
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

    def _cast_dtypes(self, df: DataFrame, description: list) -> DataFrame:
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
        columns = df.columns
        df.columns = list(range(len(columns)))
        for column_index, column_name in enumerate(df.columns):
            col = df[column_name]
            if str(col.dtype) == 'object':
                pg_type = types.get(description[column_index].type_code)
                if pg_type is not None and pg_type.name in types_map:
                    col = col.fillna(0)
                    try:
                        df[column_name] = col.astype(types_map[pg_type.name])
                    except ValueError as e:
                        logger.error(f'Error casting column {col.name} to {types_map[pg_type.name]}: {e}')
        df.columns = columns

    @profiler.profile()
    def native_query(self, query: str, params=None) -> Response:
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
                if params is not None:
                    cur.executemany(query, params)
                else:
                    cur.execute(query)
                if cur.pgresult is None or ExecStatus(cur.pgresult.status) == ExecStatus.COMMAND_OK:
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

    def insert(self, table_name: str, df: pd.DataFrame):
        need_to_close = not self.is_connected

        connection = self.connect()

        columns = [f'"{c}"' for c in df.columns]
        with connection.cursor() as cur:
            try:
                with cur.copy(f'copy "{table_name}" ({",".join(columns)}) from STDIN  WITH CSV') as copy:
                    df.to_csv(copy, index=False, header=False)

                connection.commit()
            except Exception as e:
                logger.error(f'Error running insert to {table_name} on {self.database}, {e}!')
                connection.rollback()
                raise e

        if need_to_close:
            self.disconnect()

    @profiler.profile()
    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        query_str, params = self.renderer.get_exec_params(query, with_failback=True)
        logger.debug(f"Executing SQL query: {query_str}")
        return self.native_query(query_str, params)

    def get_tables(self, all: bool = False) -> Response:
        """
        Retrieves a list of all non-system tables and views in the current schema of the PostgreSQL database.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        all_filter = 'and table_schema = current_schema()'
        if all is True:
            all_filter = ''
        query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('information_schema', 'pg_catalog')
                and table_type in ('BASE TABLE', 'VIEW')
                {all_filter}
        """
        return self.native_query(query)

    def get_columns(self, table_name: str, schema_name: Optional[str] = None) -> Response:
        """
        Retrieves column details for a specified table in the PostgreSQL database.

        Args:
            table_name (str): The name of the table for which to retrieve column information.
            schema_name (str): The name of the schema in which the table is located.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.

        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """

        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")
        if isinstance(schema_name, str):
            schema_name = f"'{schema_name}'"
        else:
            schema_name = 'current_schema()'
        query = f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
            AND
                table_schema = {schema_name}
        """
        return self.native_query(query)

    def subscribe(self, stop_event, callback, table_name, columns=None, **kwargs):
        config = self._make_connection_args()
        config['autocommit'] = True

        conn = psycopg.connect(connect_timeout=10, **config)

        # create db trigger
        trigger_name = f'mdb_notify_{table_name}'

        before, after = '', ''

        if columns:
            # check column exist
            conn.execute(f'select {",".join(columns)} from {table_name} limit 0')

            columns = set(columns)
            trigger_name += '_' + '_'.join(columns)

            news, olds = [], []
            for column in columns:
                news.append(f'NEW.{column}')
                olds.append(f'OLD.{column}')

            before = f'IF ({", ".join(news)}) IS DISTINCT FROM ({", ".join(olds)}) then\n'
            after = '\nEND IF;'
        else:
            columns = set()

        func_code = f'''
             CREATE OR REPLACE FUNCTION {trigger_name}()
               RETURNS trigger AS $$
             DECLARE
             BEGIN
               {before}
               PERFORM pg_notify( '{trigger_name}', row_to_json(NEW)::text);
               {after}
               RETURN NEW;
             END;
             $$ LANGUAGE plpgsql;
         '''
        conn.execute(func_code)

        # for after update - new and old have the same values
        conn.execute(f'''
             CREATE OR REPLACE TRIGGER {trigger_name}
               BEFORE INSERT OR UPDATE ON {table_name}
               FOR EACH ROW
               EXECUTE PROCEDURE {trigger_name}();
        ''')
        conn.commit()

        # start listen
        conn.execute(f"LISTEN {trigger_name};")

        def process_event(event):
            try:
                row = json.loads(event.payload)
            except json.JSONDecoder:
                return

            # check column in input data
            if not columns or columns.intersection(row.keys()):
                callback(row)

        try:
            conn.add_notify_handler(process_event)

            while True:
                if stop_event.is_set():
                    # exit trigger
                    return

                # trigger getting updates
                # https://www.psycopg.org/psycopg3/docs/advanced/async.html#asynchronous-notifications
                conn.execute("SELECT 1").fetchone()

                time.sleep(SUBSCRIBE_SLEEP_INTERVAL)

        finally:
            conn.execute(f'drop TRIGGER {trigger_name} on {table_name}')
            conn.execute(f'drop FUNCTION {trigger_name}')
            conn.commit()

            conn.close()
