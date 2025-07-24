import time
import json
from typing import Optional, Any

import pandas as pd
from pandas import DataFrame
import psycopg
from psycopg import Column as PGColumn, Cursor
from psycopg.postgres import TypeInfo, types as pg_types
from psycopg.pq import ExecStatus

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
import mindsdb.utilities.profiler as profiler
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE

logger = log.getLogger(__name__)

SUBSCRIBE_SLEEP_INTERVAL = 1


def _map_type(internal_type_name: str | None) -> MYSQL_DATA_TYPE:
    """Map Postgres types to MySQL types.

    Args:
        internal_type_name (str): The name of the Postgres type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type that corresponds to the Postgres type.
    """
    fallback_type = MYSQL_DATA_TYPE.VARCHAR

    if internal_type_name is None:
        return fallback_type

    internal_type_name = internal_type_name.lower()
    types_map = {
        ("smallint", "smallserial"): MYSQL_DATA_TYPE.SMALLINT,
        ("integer", "int", "serial"): MYSQL_DATA_TYPE.INT,
        ("bigint", "bigserial"): MYSQL_DATA_TYPE.BIGINT,
        ("real", "float"): MYSQL_DATA_TYPE.FLOAT,
        ("numeric", "decimal"): MYSQL_DATA_TYPE.DECIMAL,
        ("double precision",): MYSQL_DATA_TYPE.DOUBLE,
        ("character varying", "varchar"): MYSQL_DATA_TYPE.VARCHAR,
        # NOTE: if return chars-types as mysql's CHAR, then response will be padded with spaces, so return as TEXT
        ("money", "character", "char", "bpchar", "bpchar", "text"): MYSQL_DATA_TYPE.TEXT,
        ("timestamp", "timestamp without time zone", "timestamp with time zone"): MYSQL_DATA_TYPE.DATETIME,
        ("date",): MYSQL_DATA_TYPE.DATE,
        ("time", "time without time zone", "time with time zone"): MYSQL_DATA_TYPE.TIME,
        ("boolean",): MYSQL_DATA_TYPE.BOOL,
        ("bytea",): MYSQL_DATA_TYPE.BINARY,
        ("json", "jsonb"): MYSQL_DATA_TYPE.JSON,
    }

    for db_types_list, mysql_data_type in types_map.items():
        if internal_type_name in db_types_list:
            return mysql_data_type

    logger.debug(f"Postgres handler type mapping: unknown type: {internal_type_name}, use VARCHAR as fallback.")
    return fallback_type


def _make_table_response(result: list[tuple[Any]], cursor: Cursor) -> Response:
    """Build response from result and cursor.

    Args:
        result (list[tuple[Any]]): result of the query.
        cursor (psycopg.Cursor): cursor object.

    Returns:
        Response: response object.
    """
    description: list[PGColumn] = cursor.description
    mysql_types: list[MYSQL_DATA_TYPE] = []
    for column in description:
        if column.type_display == "vector":
            # 'vector' is type of pgvector extension, added here as text to not import pgvector
            # NOTE: data returned as numpy array
            mysql_types.append(MYSQL_DATA_TYPE.VECTOR)
            continue
        pg_type_info: TypeInfo = pg_types.get(column.type_code)
        if pg_type_info is None:
            # postgres may return 'polymorphic type', which are not present in the pg_types
            # list of 'polymorphic type' can be obtained:
            # SELECT oid, typname, typcategory FROM pg_type WHERE typcategory = 'P' ORDER BY oid;
            if column.type_code in (2277, 5078):
                # anyarray, anycompatiblearray
                regtype = "json"
            else:
                logger.warning(f"Postgres handler: unknown type: {column.type_code}")
                mysql_types.append(MYSQL_DATA_TYPE.TEXT)
                continue
        elif pg_type_info.array_oid == column.type_code:
            # it is any array, handle is as json
            regtype: str = "json"
        else:
            regtype: str = pg_type_info.regtype if pg_type_info is not None else None
        mysql_type = _map_type(regtype)
        mysql_types.append(mysql_type)

    # region cast int and bool to nullable types
    serieses = []
    for i, mysql_type in enumerate(mysql_types):
        expected_dtype = None
        if mysql_type in (
            MYSQL_DATA_TYPE.SMALLINT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.MEDIUMINT,
            MYSQL_DATA_TYPE.BIGINT,
            MYSQL_DATA_TYPE.TINYINT,
        ):
            expected_dtype = "Int64"
        elif mysql_type in (MYSQL_DATA_TYPE.BOOL, MYSQL_DATA_TYPE.BOOLEAN):
            expected_dtype = "boolean"
        serieses.append(pd.Series([row[i] for row in result], dtype=expected_dtype, name=description[i].name))
    df = pd.concat(serieses, axis=1, copy=False)
    # endregion

    return Response(RESPONSE_TYPE.TABLE, data_frame=df, affected_rows=cursor.rowcount, mysql_types=mysql_types)


class PostgresHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of the PostgreSQL statements.
    """

    name = "postgres"

    @profiler.profile("init_pg_handler")
    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs.get("connection_data")
        self.dialect = "postgresql"
        self.database = self.connection_args.get("database")
        self.renderer = SqlalchemyRender("postgres")

        self.connection = None
        self.is_connected = False
        self.thread_safe = False

    def __del__(self):
        if self.is_connected:
            self.disconnect()

    def _make_connection_args(self):
        config = {
            "host": self.connection_args.get("host"),
            "port": self.connection_args.get("port"),
            "user": self.connection_args.get("user"),
            "password": self.connection_args.get("password"),
            "dbname": self.connection_args.get("database"),
        }

        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        connection_parameters = self.connection_args.get("connection_parameters")
        if isinstance(connection_parameters, dict) is False:
            connection_parameters = {}
        if "connect_timeout" not in connection_parameters:
            connection_parameters["connect_timeout"] = 10
        config.update(connection_parameters)

        if self.connection_args.get("sslmode"):
            config["sslmode"] = self.connection_args.get("sslmode")

        if self.connection_args.get("autocommit"):
            config["autocommit"] = self.connection_args.get("autocommit")

        # If schema is not provided set public as default one
        if self.connection_args.get("schema"):
            config["options"] = f"-c search_path={self.connection_args.get('schema')},public"
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
            logger.error(f"Error connecting to PostgreSQL {self.database}, {e}!")
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
                cur.execute("select 1;")
            response.success = True
        except psycopg.Error as e:
            logger.error(f"Error connecting to PostgreSQL {self.database}, {e}!")
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
            "int2": "int16",
            "int4": "int32",
            "int8": "int64",
            "numeric": "float64",
            "float4": "float32",
            "float8": "float64",
        }
        columns = df.columns
        df.columns = list(range(len(columns)))
        for column_index, column_name in enumerate(df.columns):
            col = df[column_name]
            if str(col.dtype) == "object":
                pg_type_info: TypeInfo = pg_types.get(description[column_index].type_code)  # type_code is int!?
                if pg_type_info is not None and pg_type_info.name in types_map:
                    col = col.fillna(0)  # TODO rework
                    try:
                        df[column_name] = col.astype(types_map[pg_type_info.name])
                    except ValueError as e:
                        logger.error(f"Error casting column {col.name} to {types_map[pg_type_info.name]}: {e}")
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
                    response = Response(RESPONSE_TYPE.OK, affected_rows=cur.rowcount)
                else:
                    result = cur.fetchall()
                    response = _make_table_response(result, cur)
                connection.commit()
            except Exception as e:
                logger.error(f"Error running query: {query} on {self.database}, {e}!")
                response = Response(RESPONSE_TYPE.ERROR, error_code=0, error_message=str(e))
                connection.rollback()

        if need_to_close:
            self.disconnect()

        return response

    def query_stream(self, query: ASTNode, fetch_size: int = 1000):
        """
        Executes a SQL query and stream results outside by batches

        :param query: An ASTNode representing the SQL query to be executed.
        :param fetch_size: size of the batch
        :return: generator with query results
        """
        query_str, params = self.renderer.get_exec_params(query, with_failback=True)

        need_to_close = not self.is_connected

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                if params is not None:
                    cur.executemany(query_str, params)
                else:
                    cur.execute(query_str)

                if cur.pgresult is not None and ExecStatus(cur.pgresult.status) != ExecStatus.COMMAND_OK:
                    while True:
                        result = cur.fetchmany(fetch_size)
                        if not result:
                            break
                        df = DataFrame(result, columns=[x.name for x in cur.description])
                        self._cast_dtypes(df, cur.description)
                        yield df
                connection.commit()
            finally:
                connection.rollback()

        if need_to_close:
            self.disconnect()

    def insert(self, table_name: str, df: pd.DataFrame) -> Response:
        need_to_close = not self.is_connected

        connection = self.connect()

        columns = df.columns

        resp = self.get_columns(table_name)

        # copy requires precise cases of names: get current column names from table and adapt input dataframe columns
        if resp.data_frame is not None and not resp.data_frame.empty:
            db_columns = {c.lower(): c for c in resp.data_frame["COLUMN_NAME"]}

            # try to get case of existing column
            columns = [db_columns.get(c.lower(), c) for c in columns]

        columns = [f'"{c}"' for c in columns]
        rowcount = None

        with connection.cursor() as cur:
            try:
                with cur.copy(f'copy "{table_name}" ({",".join(columns)}) from STDIN WITH CSV') as copy:
                    df.to_csv(copy, index=False, header=False)

                connection.commit()
            except Exception as e:
                logger.error(f"Error running insert to {table_name} on {self.database}, {e}!")
                connection.rollback()
                raise e
            rowcount = cur.rowcount

        if need_to_close:
            self.disconnect()

        return Response(RESPONSE_TYPE.OK, affected_rows=rowcount)

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
        all_filter = "and table_schema = current_schema()"
        if all is True:
            all_filter = ""
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
            schema_name = "current_schema()"
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
            AND
                table_schema = {schema_name}
        """
        result = self.native_query(query)
        result.to_columns_table_response(map_type_fn=_map_type)
        return result

    def subscribe(self, stop_event, callback, table_name, columns=None, **kwargs):
        config = self._make_connection_args()
        config["autocommit"] = True

        conn = psycopg.connect(**config)

        # create db trigger
        trigger_name = f"mdb_notify_{table_name}"

        before, after = "", ""

        if columns:
            # check column exist
            conn.execute(f"select {','.join(columns)} from {table_name} limit 0")

            columns = set(columns)
            trigger_name += "_" + "_".join(columns)

            news, olds = [], []
            for column in columns:
                news.append(f"NEW.{column}")
                olds.append(f"OLD.{column}")

            before = f"IF ({', '.join(news)}) IS DISTINCT FROM ({', '.join(olds)}) then\n"
            after = "\nEND IF;"
        else:
            columns = set()

        func_code = f"""
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
         """
        conn.execute(func_code)

        # for after update - new and old have the same values
        conn.execute(f"""
             CREATE OR REPLACE TRIGGER {trigger_name}
               BEFORE INSERT OR UPDATE ON {table_name}
               FOR EACH ROW
               EXECUTE PROCEDURE {trigger_name}();
        """)
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
            conn.execute(f"drop TRIGGER {trigger_name} on {table_name}")
            conn.execute(f"drop FUNCTION {trigger_name}")
            conn.commit()

            conn.close()

    def meta_get_tables(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves metadata information about the tables in the PostgreSQL database to be stored in the data catalog.

        Args:
            table_names (list): A list of table names for which to retrieve metadata information.

        Returns:
            Response: A response object containing the metadata information, formatted as per the `Response` class.
        """
        query = """
            SELECT
                t.table_name,
                t.table_schema,
                t.table_type,
                obj_description(pgc.oid, 'pg_class') AS table_description,
                pgc.reltuples AS row_count
            FROM information_schema.tables t
            JOIN pg_catalog.pg_class pgc ON pgc.relname = t.table_name
            JOIN pg_catalog.pg_namespace pgn ON pgn.oid = pgc.relnamespace
            WHERE t.table_schema = current_schema()
            AND t.table_type in ('BASE TABLE', 'VIEW')
            AND t.table_name NOT LIKE 'pg_%'
            AND t.table_name NOT LIKE 'sql_%'
        """

        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t}'" for t in table_names]
            query += f" AND t.table_name IN ({','.join(table_names)})"

        result = self.native_query(query)
        return result

    def meta_get_columns(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve column metadata.

        Returns:
            Response: A response object containing the column metadata.
        """
        query = """
            SELECT
                c.table_name,
                c.column_name,
                c.data_type,
                col_description(pgc.oid, c.ordinal_position) AS column_description,
                c.column_default,
                (c.is_nullable = 'YES') AS is_nullable
            FROM information_schema.columns c
            JOIN pg_catalog.pg_class pgc ON pgc.relname = c.table_name
            JOIN pg_catalog.pg_namespace pgn ON pgn.oid = pgc.relnamespace
            WHERE c.table_schema = current_schema()
            AND pgc.relkind = 'r'  -- Only consider regular tables (avoids indexes, sequences, etc.)
            AND c.table_name NOT LIKE 'pg_%'
            AND c.table_name NOT LIKE 'sql_%'
            AND pgn.nspname = c.table_schema
        """

        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t}'" for t in table_names]
            query += f" AND c.table_name IN ({','.join(table_names)})"

        result = self.native_query(query)
        return result

    def meta_get_column_statistics(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves column statistics (e.g., most common values, frequencies, null percentage, and distinct value count)
        for the specified tables or all tables if no list is provided.

        Args:
            table_names (list): A list of table names for which to retrieve column statistics.

        Returns:
            Response: A response object containing the column statistics.
        """
        table_filter = ""
        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t}'" for t in table_names]
            table_filter = f" AND ps.tablename IN ({','.join(quoted_names)})"

        query = (
            """
            SELECT
                ps.tablename AS TABLE_NAME,
                ps.attname AS COLUMN_NAME,
                ROUND(ps.null_frac::numeric * 100, 2) AS NULL_PERCENTAGE,
                CASE 
                    WHEN ps.n_distinct < 0 THEN NULL
                    ELSE ps.n_distinct::bigint
                END AS DISTINCT_VALUES_COUNT,
                ps.most_common_vals AS MOST_COMMON_VALUES,
                ps.most_common_freqs AS MOST_COMMON_FREQUENCIES,
                ps.histogram_bounds
            FROM pg_stats ps
            WHERE ps.schemaname = current_schema()
            AND ps.tablename NOT LIKE 'pg_%'
            AND ps.tablename NOT LIKE 'sql_%'
        """
            + table_filter
            + """
            ORDER BY ps.tablename, ps.attname
        """
        )

        result = self.native_query(query)

        if result.type == RESPONSE_TYPE.TABLE and result.data_frame is not None:
            df = result.data_frame

            # Extract min/max from histogram bounds
            def extract_min_max(histogram_str):
                if histogram_str and str(histogram_str) != "nan":
                    clean = str(histogram_str).strip("{}")
                    if clean:
                        values = clean.split(",")
                        min_val = values[0].strip(" \"'") if values else None
                        max_val = values[-1].strip(" \"'") if values else None
                        return min_val, max_val
                return None, None

            min_max_values = df["histogram_bounds"].apply(extract_min_max)
            df["MINIMUM_VALUE"] = min_max_values.apply(lambda x: x[0])
            df["MAXIMUM_VALUE"] = min_max_values.apply(lambda x: x[1])

        result.data_frame = df.drop(columns=["histogram_bounds"])
        return result

    def meta_get_primary_keys(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves primary key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve primary key information.

        Returns:
            Response: A response object containing the primary key information.
        """
        query = """
            SELECT
                tc.table_name,
                kcu.column_name,
                kcu.ordinal_position,
                tc.constraint_name
            FROM
                information_schema.table_constraints AS tc
            JOIN
                information_schema.key_column_usage AS kcu
            ON
                tc.constraint_name = kcu.constraint_name
            WHERE
                tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = current_schema()
        """

        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t}'" for t in table_names]
            query += f" AND tc.table_name IN ({','.join(table_names)})"

        result = self.native_query(query)
        return result

    def meta_get_foreign_keys(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves foreign key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve foreign key information.

        Returns:
            Response: A response object containing the foreign key information.
        """
        query = """
            SELECT
                ccu.table_name AS parent_table_name,
                ccu.column_name AS parent_column_name,
                tc.table_name AS child_table_name,
                kcu.column_name AS child_column_name,
                tc.constraint_name
            FROM
                information_schema.table_constraints AS tc
            JOIN
                information_schema.key_column_usage AS kcu
            ON
                tc.constraint_name = kcu.constraint_name
            JOIN
                information_schema.constraint_column_usage AS ccu
            ON
                ccu.constraint_name = tc.constraint_name
            WHERE
                tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = current_schema()
        """

        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t}'" for t in table_names]
            query += f" AND tc.table_name IN ({','.join(table_names)})"

        result = self.native_query(query)
        return result
