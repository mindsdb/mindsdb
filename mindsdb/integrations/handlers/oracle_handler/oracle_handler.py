from typing import Any, Dict, List, Optional, Text

import oracledb
import pandas as pd
from oracledb import connect, Connection, DatabaseError, Cursor
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
import mindsdb.utilities.profiler as profiler
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


oracledb.defaults.fetch_lobs = False  # Return LOBs directly as strings or bytes.
logger = log.getLogger(__name__)


def _map_type(internal_type_name: str) -> MYSQL_DATA_TYPE:
    """Map Oracle types to MySQL types.
        List of types: https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html

    Args:
        internal_type_name (str): The name of the Oracle type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type that corresponds to the Oracle type.
    """
    internal_type_name = internal_type_name.upper()
    types_map = {
        (
            "VARCHAR2",
            "NVARCHAR2",
            "CHARACTER VARYING",
            "CHAR VARYING",
            "NATIONAL CHARACTER",
            "NATIONAL CHAR",
            "VARCHAR",
            "NATIONAL CHARACTER VARYING",
            "NATIONAL CHAR VARYING",
            "NCHAR VARYING",
            "LONG VARCHAR",
        ): MYSQL_DATA_TYPE.VARCHAR,
        ("INTEGER", "INT"): MYSQL_DATA_TYPE.INT,
        ("SMALLINT",): MYSQL_DATA_TYPE.SMALLINT,
        ("NUMBER", "DECIMAL"): MYSQL_DATA_TYPE.DECIMAL,
        ("FLOAT", "BINARY_FLOAT", "REAL"): MYSQL_DATA_TYPE.FLOAT,
        ("BINARY_DOUBLE",): MYSQL_DATA_TYPE.DOUBLE,
        ("LONG",): MYSQL_DATA_TYPE.BIGINT,
        ("DATE",): MYSQL_DATA_TYPE.DATE,
        (
            "HOUR",
            "MINUTE",
            "SECOND",
            "TIMEZONE_HOUR",
            "TIMEZONE_MINUTE",
        ): MYSQL_DATA_TYPE.SMALLINT,
        (
            "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP WITH LOCAL TIME ZONE",
        ): MYSQL_DATA_TYPE.TIMESTAMP,
        ("RAW", "LONG RAW", "BLOB", "BFILE"): MYSQL_DATA_TYPE.BINARY,
        ("ROWID", "UROWID"): MYSQL_DATA_TYPE.TEXT,
        ("CHAR", "NCHAR", "CLOB", "NCLOB", "CHARACTER"): MYSQL_DATA_TYPE.CHAR,
        ("VECTOR",): MYSQL_DATA_TYPE.VECTOR,
        ("JSON",): MYSQL_DATA_TYPE.JSON,
    }

    for db_types_list, mysql_data_type in types_map.items():
        if internal_type_name in db_types_list:
            return mysql_data_type

    logger.debug(f"Oracle handler type mapping: unknown type: {internal_type_name}, use VARCHAR as fallback.")
    return MYSQL_DATA_TYPE.VARCHAR


def _make_table_response(result: list[tuple[Any]], cursor: Cursor) -> Response:
    """Build response from result and cursor.

    Args:
        result (list[tuple[Any]]): result of the query.
        cursor (oracledb.Cursor): cursor object.

    Returns:
        Response: response object.
    """
    description: list[tuple[Any]] = cursor.description
    mysql_types: list[MYSQL_DATA_TYPE] = []
    for column in description:
        db_type = column[1]
        precision = column[4]
        scale = column[5]
        if db_type is oracledb.DB_TYPE_JSON:
            mysql_types.append(MYSQL_DATA_TYPE.JSON)
        elif db_type is oracledb.DB_TYPE_VECTOR:
            mysql_types.append(MYSQL_DATA_TYPE.VECTOR)
        elif db_type is oracledb.DB_TYPE_NUMBER:
            if scale != 0:
                mysql_types.append(MYSQL_DATA_TYPE.FLOAT)
            else:
                # python max int is 19 digits, oracle can return more
                if precision > 18:
                    mysql_types.append(MYSQL_DATA_TYPE.DECIMAL)
                else:
                    mysql_types.append(MYSQL_DATA_TYPE.INT)
        elif db_type is oracledb.DB_TYPE_BINARY_FLOAT:
            mysql_types.append(MYSQL_DATA_TYPE.FLOAT)
        elif db_type is oracledb.DB_TYPE_BINARY_DOUBLE:
            mysql_types.append(MYSQL_DATA_TYPE.FLOAT)
        elif db_type is oracledb.DB_TYPE_BINARY_INTEGER:
            mysql_types.append(MYSQL_DATA_TYPE.INT)
        elif db_type is oracledb.DB_TYPE_BOOLEAN:
            mysql_types.append(MYSQL_DATA_TYPE.BOOLEAN)
        elif db_type in (
            oracledb.DB_TYPE_CHAR,
            oracledb.DB_TYPE_NCHAR,
            oracledb.DB_TYPE_LONG,
            oracledb.DB_TYPE_NVARCHAR,
            oracledb.DB_TYPE_VARCHAR,
            oracledb.DB_TYPE_LONG_NVARCHAR,
        ):
            mysql_types.append(MYSQL_DATA_TYPE.TEXT)
        elif db_type in (oracledb.DB_TYPE_RAW, oracledb.DB_TYPE_LONG_RAW):
            mysql_types.append(MYSQL_DATA_TYPE.BINARY)
        elif db_type is oracledb.DB_TYPE_DATE:
            mysql_types.append(MYSQL_DATA_TYPE.DATE)
        elif db_type is oracledb.DB_TYPE_TIMESTAMP:
            mysql_types.append(MYSQL_DATA_TYPE.TIMESTAMP)
        else:
            # fallback
            mysql_types.append(MYSQL_DATA_TYPE.TEXT)

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
        serieses.append(pd.Series([row[i] for row in result], dtype=expected_dtype, name=description[i][0]))
    df = pd.concat(serieses, axis=1, copy=False)
    # endregion

    return Response(RESPONSE_TYPE.TABLE, data_frame=df, mysql_types=mysql_types)


class OracleHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of SQL queries on Oracle.
    """

    name = "oracle"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to OracleDB.
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
        if not all(key in self.connection_data for key in ["user", "password"]):
            raise ValueError("Required parameters (user, password) must be provided.")

        if self.connection_data.get("thick_mode", False):
            oracle_client_lib_dir = self.connection_data.get("oracle_client_lib_dir")
            if isinstance(oracle_client_lib_dir, str) and oracle_client_lib_dir.strip():
                try:
                    oracledb.init_oracle_client(lib_dir=oracle_client_lib_dir)
                except Exception as e:
                    raise ValueError(f"Failed to initialize Oracle client: {e}")
            else:
                raise ValueError(
                    "Parameter 'oracle_client_lib_dir' must be provided as a non-empty string when using thick_mode."
                )

        config = {
            "user": self.connection_data["user"],
            "password": self.connection_data["password"],
        }

        # If 'dsn' is given, use it. Otherwise, use the individual connection parameters.
        if "dsn" in self.connection_data:
            config["dsn"] = self.connection_data["dsn"]

        else:
            if "host" not in self.connection_data and not any(
                key in self.connection_data for key in ["sid", "service_name"]
            ):
                raise ValueError(
                    "Required parameter host and either sid or service_name must be provided. Alternatively, dsn can be provided."
                )

            config["host"] = self.connection_data.get("host")

            # Optional connection parameters when 'dsn' is not given.
            optional_parameters = ["port", "sid", "service_name"]
            for parameter in optional_parameters:
                if parameter in self.connection_data:
                    config[parameter] = self.connection_data[parameter]

        # Other optional connection parameters.
        if "disable_oob" in self.connection_data:
            config["disable_oob"] = self.connection_data["disable_oob"]

        if "auth_mode" in self.connection_data:
            mode_name = "AUTH_MODE_" + self.connection_data["auth_mode"].upper()
            if not hasattr(oracledb, mode_name):
                raise ValueError(f"Unknown auth mode: {mode_name}")
            config["mode"] = getattr(oracledb, mode_name)

        try:
            connection = connect(
                **config,
            )

            if "session_variables" in self.connection_data:
                with connection.cursor() as cur:
                    for key, value in self.connection_data["session_variables"].items():
                        cur.execute(f"ALTER SESSION SET {key} = {repr(value)}")

        except DatabaseError as database_error:
            logger.error(f"Error connecting to Oracle, {database_error}!")
            raise

        except Exception as unknown_error:
            logger.error(f"Unknown error when connecting to Oracle: {unknown_error}")
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
            logger.error(f"Connection check to Oracle failed, {known_error}!")
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f"Connection check to Oracle failed due to an unknown error, {unknown_error}!")
            response.error_message = str(unknown_error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    @profiler.profile()
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
                    response = _make_table_response(result, cur)
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

    def query_stream(self, query: ASTNode, fetch_size: int = 1000):
        """
        Executes a SQL query represented by an ASTNode and retrieves the data in a streaming fashion.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.
            fetch_size (int): The number of rows to fetch in each batch.
        Yields:
            pd.DataFrame: A DataFrame containing a batch of rows from the query result.
            Response: In case of an error, yields a Response object with the error details.
        """
        query_str = SqlalchemyRender("oracle").get_string(query, with_failback=True)
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query_str)
                while True:
                    result = cur.fetchmany(fetch_size)
                    if not result:
                        break
                    df = pd.DataFrame(result, columns=[col[0] for col in cur.description])
                    yield df
                connection.commit()
            finally:
                connect
        if need_to_close is True:
            self.disconnect()

    def insert(self, table_name: str, df: pd.DataFrame) -> Response:
        """
        Inserts data from a DataFrame into a specified table in the Oracle database.

        Args:
            table_name (str): The name of the table where the data will be inserted.
            df (pd.DataFrame): The DataFrame containing the data to be inserted.
        Returns:
            Response: A response object indicating the success or failure of the insert operation.
        """
        need_to_close = self.is_connected is False
        connection = self.connect()
        columns = list(df.columns)
        placeholders = ", ".join([f":{i + 1}" for i in range(len(columns))])
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        with connection.cursor() as cur:
            try:
                cur.executemany(insert_query, df.values.tolist())
                connection.commit()
                rowcount = cur.rowcount
            except DatabaseError as database_error:
                logger.error(f"Error inserting data into table {table_name} on Oracle, {database_error}!")
                connection.rollback()
                raise
        if need_to_close is True:
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
        renderer = SqlalchemyRender("oracle")
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables and views in the current schema of the Oracle database.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        query = """
            SELECT
                owner AS table_schema,
                table_name AS table_name,
                'BASE TABLE' AS table_type
            FROM all_tables t
            JOIN all_users u ON t.owner = u.username
            WHERE t.tablespace_name = 'USERS'

            UNION ALL

            SELECT
                v.owner AS table_schema,
                v.view_name AS table_name,
                'VIEW' AS table_type
            FROM all_views v
            JOIN all_users u ON v.owner = u.username
            WHERE v.owner IN (
                SELECT DISTINCT owner
                FROM all_tables
                WHERE tablespace_name = 'USERS'
            )
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
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_ID AS ORDINAL_POSITION,
                DATA_DEFAULT AS COLUMN_DEFAULT,
                CASE NULLABLE WHEN 'Y' THEN 'YES' ELSE 'NO' END AS IS_NULLABLE,
                CHAR_LENGTH AS CHARACTER_MAXIMUM_LENGTH,
                NULL AS CHARACTER_OCTET_LENGTH,
                DATA_PRECISION AS NUMERIC_PRECISION,
                DATA_SCALE AS NUMERIC_SCALE,
                NULL AS DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                NULL AS COLLATION_NAME
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{table_name}'
            ORDER BY TABLE_NAME, COLUMN_ID
        """
        result = self.native_query(query)
        if result.resp_type is RESPONSE_TYPE.TABLE:
            result.to_columns_table_response(map_type_fn=_map_type)
        return result

    def meta_get_tables(self, table_names: Optional[List[str]]) -> Response:
        """
        Retrieves metadata about all non-system tables and views in the current schema of the Oracle database.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each containing metadata about a table or view.
        """
        query = """
            SELECT
                o.object_name AS table_name,
                USER AS table_schema,
                o.object_type AS table_type,
                c.comments AS table_description,
                t.num_rows AS row_count
            FROM
                user_objects o
            LEFT JOIN
                user_tab_comments c ON o.object_name = c.table_name
            LEFT JOIN
                user_tables t ON o.object_name = t.table_name AND o.object_type = 'TABLE'
            WHERE
                o.object_type IN ('TABLE', 'VIEW')
        """
        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t.upper()}'" for t in table_names]
            query += f" AND o.object_name IN ({','.join(table_names)})"

        query += " ORDER BY o.object_name"

        result = self.native_query(query)
        return result

    def meta_get_columns(self, table_names: Optional[List[str]]) -> Response:
        """Retrieves metadata about the columns of specified tables in the Oracle database.

        Args:
            table_names (list[str]): A list of table names for which to retrieve column metadata.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each containing metadata about a column.
        """
        query = """
            SELECT
                utc.table_name,
                utc.column_name,
                utc.data_type,
                ucc.comments AS column_description,
                utc.data_default AS column_default,
                CASE
                    WHEN utc.nullable = 'Y' THEN 1
                    ELSE 0
                END AS is_nullable
            FROM
                user_tab_columns utc
            JOIN
                user_tables ut ON utc.table_name = ut.table_name
            LEFT JOIN
                user_col_comments ucc ON utc.table_name = ucc.table_name AND utc.column_name = ucc.column_name
        """
        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t.upper()}'" for t in table_names]
            query += f" WHERE utc.table_name IN ({','.join(table_names)})"
        query += " ORDER BY utc.table_name, utc.column_id"
        result = self.native_query(query)
        return result

    def meta_get_column_statistics(self, table_names: Optional[List[str]]) -> Response:
        """Retrieves statistics about the columns of specified tables in the Oracle database.

        Args:
            table_names (list[str]): A list of table names for which to retrieve column statistics.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each containing statistics about a column.
        """
        table_filter = ""
        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t.upper()}'" for t in table_names]
            table_filter = f" WHERE cs.table_name IN ({','.join(quoted_names)})"

        query = (
            """
            SELECT
                cs.table_name AS TABLE_NAME,
                cs.column_name AS COLUMN_NAME,
                CASE
                    WHEN cs.sample_size > 0 THEN ROUND((cs.num_nulls / cs.sample_size) * 100, 2)
                    ELSE NULL
                END AS NULL_PERCENTAGE,
                cs.num_distinct AS DISTINCT_VALUES_COUNT,
                NULL AS MOST_COMMON_VALUES,
                NULL AS MOST_COMMON_FREQUENCIES,
                cs.histogram AS HISTOGRAM_TYPE,
                h.bounds AS HISTOGRAM_BOUNDS
            FROM
                user_tab_col_statistics cs
            LEFT JOIN (
                SELECT
                    table_name,
                    column_name,
                    LISTAGG(endpoint_value, ', ') WITHIN GROUP (ORDER BY endpoint_number) AS bounds
                FROM
                    user_tab_histograms
                GROUP BY
                    table_name,
                    column_name
            ) h ON cs.table_name = h.table_name AND cs.column_name = h.column_name
            """
            + table_filter
            + """
            ORDER BY
                cs.table_name,
                cs.column_name
            """
        )

        result = self.native_query(query)

        if result.resp_type is RESPONSE_TYPE.TABLE and result.data_frame is not None:
            df = result.data_frame

            def extract_min_max(
                histogram_str: str,
            ) -> tuple[Optional[float], Optional[float]]:
                if histogram_str and str(histogram_str).lower() not in ["nan", "none"]:
                    values = str(histogram_str).split(",")
                    if values:
                        min_val = values[0].strip(" '\"")
                        max_val = values[-1].strip(" '\"")
                        return min_val, max_val
                return None, None

            min_max_values = df["HISTOGRAM_BOUNDS"].apply(extract_min_max)
            df["MINIMUM_VALUE"] = min_max_values.apply(lambda x: x[0])
            df["MAXIMUM_VALUE"] = min_max_values.apply(lambda x: x[1])
            df.drop(columns=["HISTOGRAM_BOUNDS"], inplace=True)
        return result

    def meta_get_primary_keys(self, table_names: Optional[List[str]]) -> Response:
        """
        Retrieves the primary keys for the specified tables in the Oracle database.

        Args:
            table_names (list[str]): A list of table names for which to retrieve primary keys.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each containing information about a primary key.
        """

        query = """
            SELECT
                cols.table_name,
                cols.column_name,
                cols.position AS ordinal_position,
                cons.constraint_name
            FROM
                all_constraints cons
            JOIN
                all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
            WHERE
                cons.constraint_type = 'P'
                AND cons.owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
        """
        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t.upper()}'" for t in table_names]
            query += f" AND cols.table_name IN ({','.join(quoted_names)})"

        query += " ORDER BY cols.table_name, cols.position"

        result = self.native_query(query)
        return result

    def meta_get_foreign_keys(self, table_names: Optional[List[str]]) -> Response:
        """
        Retrieves the foreign keys for the specified tables in the Oracle database.

        Args:
            table_names (list[str]): A list of table names for which to retrieve foreign keys.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each containing information about a foreign key.
        """

        query = """
        SELECT
            pk_cols.table_name AS parent_table_name,
            pk_cols.column_name AS parent_column_name,
            fk_cols.table_name AS child_table_name,
            fk_cols.column_name AS child_column_name,
            fk_cons.constraint_name
        FROM
            all_constraints fk_cons
        JOIN
            all_cons_columns fk_cols ON fk_cons.owner = fk_cols.owner AND fk_cons.constraint_name = fk_cols.constraint_name
        JOIN
            all_cons_columns pk_cols ON fk_cons.owner = pk_cols.owner AND fk_cons.r_constraint_name = pk_cols.constraint_name
        WHERE
            fk_cons.constraint_type = 'R'
            AND fk_cons.owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
        """
        if table_names is not None and len(table_names) > 0:
            quoted_names = [f"'{t.upper()}'" for t in table_names]
            query += f" AND fk_cols.table_name IN ({','.join(quoted_names)})"

        query += " ORDER BY fk_cols.table_name, fk_cols.position"
        result = self.native_query(query)
        return result
