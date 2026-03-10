from typing import Any, Optional, List, Generator

import pandas
from pandas import DataFrame
from pandas.api import types as pd_types
from snowflake.sqlalchemy import snowdialect
from snowflake import connector
from snowflake.connector.errors import NotSupportedError
from snowflake.connector.cursor import ResultMetadata

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Select, Identifier

from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.utilities.types.column import Column
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    TableResponse,
    OkResponse,
    ErrorResponse,
    DataHandlerResponse,
)

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE

from .auth_types import (
    PasswordAuthType,
    KeyPairAuthType,
)

try:
    import pyarrow as pa

    memory_pool = pa.default_memory_pool()
except Exception:
    memory_pool = None


logger = log.getLogger(__name__)


def _map_type(internal_type_name: str) -> MYSQL_DATA_TYPE:
    """Map Snowflake types to MySQL types.

    Args:
        internal_type_name (str): The name of the Snowflake type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type that corresponds to the Snowflake type.
    """
    internal_type_name = internal_type_name.upper()
    types_map = {
        ("NUMBER", "DECIMAL", "DEC", "NUMERIC"): MYSQL_DATA_TYPE.DECIMAL,
        ("INT , INTEGER , BIGINT , SMALLINT , TINYINT , BYTEINT"): MYSQL_DATA_TYPE.INT,
        ("FLOAT", "FLOAT4", "FLOAT8", "FIXED"): MYSQL_DATA_TYPE.FLOAT,
        ("DOUBLE", "DOUBLE PRECISION", "REAL"): MYSQL_DATA_TYPE.DOUBLE,
        ("VARCHAR",): MYSQL_DATA_TYPE.VARCHAR,
        ("CHAR", "CHARACTER", "NCHAR"): MYSQL_DATA_TYPE.CHAR,
        ("STRING", "TEXT", "NVARCHAR"): MYSQL_DATA_TYPE.TEXT,
        ("NVARCHAR2", "CHAR VARYING", "NCHAR VARYING"): MYSQL_DATA_TYPE.VARCHAR,
        ("BINARY", "VARBINARY"): MYSQL_DATA_TYPE.BINARY,
        ("BOOLEAN",): MYSQL_DATA_TYPE.BOOL,
        ("TIMESTAMP_NTZ", "DATETIME"): MYSQL_DATA_TYPE.DATETIME,
        ("DATE",): MYSQL_DATA_TYPE.DATE,
        ("TIME",): MYSQL_DATA_TYPE.TIME,
        ("TIMESTAMP_LTZ",): MYSQL_DATA_TYPE.DATETIME,
        ("TIMESTAMP_TZ",): MYSQL_DATA_TYPE.DATETIME,
        ("OBJECT", "ARRAY"): MYSQL_DATA_TYPE.JSON,
        ("VECTOR",): MYSQL_DATA_TYPE.VECTOR,
        ("VARIANT", "MAP", "GEOGRAPHY", "GEOMETRY", "VECTOR"): MYSQL_DATA_TYPE.VARCHAR,
    }

    for db_types_list, mysql_data_type in types_map.items():
        if internal_type_name in db_types_list:
            return mysql_data_type

    logger.debug(f"Snowflake handler type mapping: unknown type: {internal_type_name}, use VARCHAR as fallback.")
    return MYSQL_DATA_TYPE.VARCHAR


def _get_columns(description: list[ResultMetadata], sample: pandas.DataFrame = None) -> list[Column]:
    """Get columns from Snowflake cursor description.

    Args:
        description (list[ResultMetadata]): cursor description metadata.
        sample (pandas.DataFrame): data sample

    Returns:
        list[Column]: list of columns with mapped MySQL types.
    """
    result = []
    for column in description:
        mysql_type = None
        sf_type_name = connector.constants.FIELD_ID_TO_NAME.get(column.type_code)
        if sf_type_name is None:
            logger.warning(f"Snowflake handler: unknown type code: {column.type_code}")
            mysql_type = MYSQL_DATA_TYPE.VARCHAR

        if sample is not None:
            column_dtype = sample[column.name].dtype

            if pd_types.is_integer_dtype(column_dtype):
                column_dtype_name = column_dtype.name
                if column_dtype_name in ("int8", "Int8"):
                    mysql_type = MYSQL_DATA_TYPE.TINYINT
                elif column_dtype in ("int16", "Int16"):
                    mysql_type = MYSQL_DATA_TYPE.SMALLINT
                elif column_dtype in ("int32", "Int32"):
                    mysql_type = MYSQL_DATA_TYPE.MEDIUMINT
                elif column_dtype in ("int64", "Int64"):
                    mysql_type = MYSQL_DATA_TYPE.BIGINT
                else:
                    mysql_type = MYSQL_DATA_TYPE.INT

            elif pd_types.is_float_dtype(column_dtype):
                column_dtype_name = column_dtype.name
                if column_dtype_name in ("float16", "Float16"):  # Float16 does not exists so far
                    mysql_type = MYSQL_DATA_TYPE.FLOAT
                elif column_dtype_name in ("float32", "Float32"):
                    mysql_type = MYSQL_DATA_TYPE.FLOAT
                elif column_dtype_name in ("float64", "Float64"):
                    mysql_type = MYSQL_DATA_TYPE.DOUBLE
                else:
                    mysql_type = MYSQL_DATA_TYPE.FLOAT

            elif pd_types.is_bool_dtype(column_dtype):
                mysql_type = MYSQL_DATA_TYPE.BOOLEAN

            elif pd_types.is_datetime64_any_dtype(column_dtype):
                mysql_type = MYSQL_DATA_TYPE.DATETIME
                series = sample[column.name]
                # snowflake use pytz.timezone
                if series.dt.tz is not None and getattr(series.dt.tz, "zone", "UTC") != "UTC":
                    series = series.dt.tz_convert("UTC")
                    sample[column.name] = series.dt.tz_localize(None)

            elif pd_types.is_object_dtype(column_dtype):
                if sf_type_name == "TEXT":
                    # we can also check column.internal_size, if == 16777216 then it is TEXT, else VARCHAR(internal_size)
                    mysql_type = MYSQL_DATA_TYPE.TEXT
                elif sf_type_name == "BINARY":
                    # if column.internal_size == 8388608 then BINARY, else VARBINARY(internal_size)
                    mysql_type = MYSQL_DATA_TYPE.BINARY
                elif sf_type_name == "DATE":
                    mysql_type = MYSQL_DATA_TYPE.DATE
                elif sf_type_name == "TIME":
                    mysql_type = MYSQL_DATA_TYPE.TIME
                elif sf_type_name == "FIXED":
                    if getattr(column, "scale", None) == 0:
                        mysql_type = MYSQL_DATA_TYPE.INT
                    else:
                        # It is NUMBER, DECIMAL or NUMERIC with scale > 0
                        mysql_type = MYSQL_DATA_TYPE.FLOAT

        if mysql_type is None:
            mysql_type = _map_type(sf_type_name)

        result.append(Column(name=column.name, type=mysql_type, original_type=sf_type_name))
    return result


class SnowflakeHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of the Snowflake statements.
    """

    name = "snowflake"
    stream_response = True

    _auth_types = {
        "key_pair": KeyPairAuthType(),
        "password": PasswordAuthType(),
    }

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data")
        self.renderer = SqlalchemyRender(snowdialect.dialect)

        self.is_connected = False
        self.connection = None

    def connect(self):
        """
        Establishes a connection to a Snowflake account.

        Supports two authentication methods:
        1. User/password authentication (legacy)
        2. Key pair authentication (recommended)

        Raises:
            ValueError: If the required connection parameters are not provided.
            snowflake.connector.errors.Error: If an error occurs while connecting to the Snowflake account.

        Returns:
            snowflake.connector.connection.SnowflakeConnection: A connection object to the Snowflake account.
        """

        if self.is_connected is True:
            return self.connection

        auth_type_key = self.connection_data.get("auth_type", "password")
        if auth_type_key is None:
            supported = ", ".join(self._auth_types.keys())
            raise ValueError(f"auth_type is required. Supported values: {supported}.")

        auth_type = self._auth_types.get(auth_type_key)
        if not auth_type:
            supported = ", ".join(self._auth_types.keys())
            raise ValueError(f"Invalid auth_type '{auth_type_key}'. Supported values: {supported}.")

        config = auth_type.get_config(**self.connection_data)

        try:
            self.connection = connector.connect(**config)
            self.connection.telemetry_enabled = False
            self.is_connected = True
            return self.connection
        except connector.errors.Error as e:
            logger.error(f"Error connecting to Snowflake, {e}!")
            raise

    def disconnect(self):
        """
        Closes the connection to the Snowflake account if it's currently open.
        """

        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Snowflake account.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = not self.is_connected

        try:
            connection = self.connect()

            # Execute a simple query to test the connection
            with connection.cursor() as cur:
                cur.execute("select 1;")
            response.success = True
        except (connector.errors.Error, ValueError) as e:
            logger.error(f"Error connecting to Snowflake, {e}!")
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: str, stream: bool = True, **kwargs) -> TableResponse | OkResponse | ErrorResponse:
        """Executes a SQL query on the Snowflake account and returns the result.

        Args:
            query (str): The SQL query to be executed.
            stream (bool): If True - return TableResponse with generator inside.

        Returns:
            DataHandlerResponse: A response object containing the result of the query or an error message.
        """
        generator = self._execute_fetch_batches(query)
        try:
            response: TableResponse = next(generator)
            response.data_generator = generator
            if stream is False:
                response.fetchall()
        except StopIteration as e:
            response = e.value
            if isinstance(response, DataHandlerResponse) is False:
                raise

        return response

    def _execute_fetch_batches(
        self, query: str
    ) -> Generator[TableResponse | pandas.DataFrame, None, OkResponse | ErrorResponse]:
        """Execute a SQL query and yield results in batches.

        Args:
            query (str): The SQL query to execute.

        Yields:
            TableResponse: First yield — response with column metadata and affected row count.
            pandas.DataFrame: Subsequent yields — batches of query results.

        Returns:
            OkResponse: For DML statements (INSERT/DELETE/UPDATE) with affected row count.
            ErrorResponse: If an exception occurs during query execution.
        """
        connection = self.connect()
        with connection.cursor(connector.DictCursor) as cursor:
            try:
                cursor.execute(query)
                try:
                    try:
                        batches_iter = cursor.fetch_pandas_batches()
                    except ValueError:
                        # duplicated columns raises ValueError
                        raise NotSupportedError()
                    try:
                        sample_df = next(batches_iter)
                    except StopIteration:
                        sample_df = None
                    columns = _get_columns(cursor.description, sample=sample_df)
                    yield TableResponse(data=sample_df, affected_rows=cursor.rowcount, columns=columns)
                    for batch_df in batches_iter:
                        yield batch_df
                except NotSupportedError:
                    # Fallback for CREATE/DELETE/UPDATE. These commands returns table with single column,
                    # but it cannot be retrieved as pandas DataFrame.
                    result = cursor.fetchall()
                    match result:
                        case (
                            [{"number of rows inserted": affected_rows}]
                            | [{"number of rows deleted": affected_rows}]
                            | [{"number of rows updated": affected_rows, "number of multi-joined rows updated": _}]
                        ):
                            response = OkResponse(affected_rows=affected_rows)
                        case list():
                            response = TableResponse(data=DataFrame(result, columns=[x[0] for x in cursor.description]))
                        case _:
                            # Looks like SnowFlake always returns something in response, so this is suspicious
                            logger.warning("Snowflake did not return any data in response.")
                            response = OkResponse()
                    return response
            except Exception as e:
                logger.error(f"Error running query: {query} on {self.connection_data.get('database')}, {e}!")
                return ErrorResponse(error_code=0, error_message=str(e))

        if memory_pool is not None and memory_pool.backend_name == "jemalloc":
            # This reduce memory consumption, but will slow down next query slightly.
            # Except pool type 'jemalloc': memory consumption do not change significantly
            # and next query processing time may be even lower.
            memory_pool.release_unused()

    def query(self, query: ASTNode) -> DataHandlerResponse:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            DataHandlerResponse: The response from the `native_query` method, containing the result of the SQL query execution.
        """

        query_str = self.renderer.get_string(query, with_failback=True)
        logger.debug(f"Executing SQL query: {query_str}")
        result = self.native_query(query_str)
        return self.lowercase_columns(result, query)

    def lowercase_columns(self, result, query):
        if not isinstance(query, Select) or result.data_frame is None:
            return result

        quoted_columns = []
        if query.targets is not None:
            for column in query.targets:
                if hasattr(column, "alias") and column.alias is not None:
                    if column.alias.is_quoted[-1]:
                        quoted_columns.append(column.alias.parts[-1])
                elif isinstance(column, Identifier):
                    if column.is_quoted[-1]:
                        quoted_columns.append(column.parts[-1])

        rename_columns = {}
        for col in result.data_frame.columns:
            if col.isupper() and col not in quoted_columns:
                rename_columns[col] = col.lower()
        if rename_columns:
            result.data_frame = result.data_frame.rename(columns=rename_columns)
        return result

    def get_tables(self) -> DataHandlerResponse:
        """
        Retrieves a list of all non-system tables and views in the current schema of the Snowflake account.

        Returns:
            DataHandlerResponse: A response object containing the list of tables and views.
        """

        query = """
            SELECT TABLE_NAME, TABLE_SCHEMA, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
              AND TABLE_SCHEMA = current_schema()
        """
        return self.native_query(query)

    def get_columns(self, table_name) -> DataHandlerResponse:
        """
        Retrieves column details for a specified table in the Snowflake account.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Returns:
            DataHandlerResponse: A response object containing the column details.

        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """

        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

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
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
              AND TABLE_SCHEMA = current_schema()
        """
        result = self.native_query(query)
        result.to_columns_table_response(map_type_fn=_map_type)

        return result

    def meta_get_tables(self, table_names: Optional[List[str]] = None) -> DataHandlerResponse:
        """
        Retrieves metadata information about the tables in the Snowflake database to be stored in the data catalog.

        Args:
            table_names (list): A list of table names for which to retrieve metadata information.

        Returns:
            DataHandlerResponse: A response object containing the metadata information.
        """
        query = """
            SELECT
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE,
                COMMENT AS TABLE_DESCRIPTION,
                ROW_COUNT,
                CREATED,
                LAST_ALTERED
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = current_schema()
            AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        """

        if table_names is not None and len(table_names) > 0:
            table_names_str = ", ".join([f"'{t.upper()}'" for t in table_names])
            query += f" AND TABLE_NAME IN ({table_names_str})"

        result = self.native_query(query)
        if result.data_frame is not None and "ROW_COUNT" in result.data_frame.columns:
            # Snowflake can return NULL for ROW_COUNT (e.g., for views); preserve as <NA>.
            result.data_frame["ROW_COUNT"] = result.data_frame["ROW_COUNT"].astype("Int64")
        return result

    def meta_get_columns(self, table_names: Optional[List[str]] = None) -> DataHandlerResponse:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve column metadata.

        Returns:
            DataHandlerResponse: A response object containing the column metadata.
        """
        query = """
            SELECT
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                COMMENT AS COLUMN_DESCRIPTION,
                COLUMN_DEFAULT,
                (IS_NULLABLE = 'YES') AS IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                COLLATION_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = current_schema()
        """

        if table_names is not None and len(table_names) > 0:
            table_names_str = ", ".join([f"'{t.upper()}'" for t in table_names])
            query += f" AND TABLE_NAME IN ({table_names_str})"

        result = self.native_query(query)
        return result

    def meta_get_column_statistics(self, table_names: Optional[List[str]] = None) -> DataHandlerResponse:
        """
        Retrieves basic column statistics: null %, distinct count.
        Due to Snowflake limitations, this runs per-table not per-column.
        TODO:  Add most_common_values and most_common_frequencies
        """
        columns_query = """
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = current_schema()
        """
        if table_names:
            table_names_str = ", ".join([f"'{t.upper()}'" for t in table_names])
            columns_query += f" AND TABLE_NAME IN ({table_names_str})"

        columns_result = self.native_query(columns_query)
        if (
            isinstance(columns_result, ErrorResponse)
            or columns_result.data_frame is None
            or columns_result.data_frame.empty
        ):
            return ErrorResponse(error_message="No columns found.")

        columns_df = columns_result.data_frame
        grouped = columns_df.groupby(["TABLE_SCHEMA", "TABLE_NAME"])
        all_stats = []

        for (table_schema, table_name), group in grouped:
            select_parts = []
            for _, row in group.iterrows():
                col = row["COLUMN_NAME"]
                data_type = row["DATA_TYPE"]
                # Ensure column names in the query are properly quoted if they contain special characters or are case-sensitive
                quoted_col = f'"{col}"'
                select_parts.extend(
                    [
                        f'COUNT_IF({quoted_col} IS NULL) AS "nulls_{col}"',
                        f'APPROX_COUNT_DISTINCT({quoted_col}) AS "distincts_{col}"',
                    ]
                )
                # We can sort and find min/max for array but is expensive for large tables, avoid for now
                if data_type not in {"ARRAY", "OBJECT", "VARIANT"}:
                    select_parts.extend(
                        [
                            f'MIN({quoted_col}) AS "min_{col}"',
                            f'MAX({quoted_col}) AS "max_{col}"',
                        ]
                    )

            quoted_table_name = f'"{table_schema}"."{table_name}"'
            stats_query = f"""
            SELECT COUNT(*) AS "total_rows", {", ".join(select_parts)}
            FROM {quoted_table_name}
            """
            try:
                stats_res = self.native_query(stats_query)
                if (
                    not isinstance(stats_res, TableResponse)
                    or stats_res.data_frame is None
                    or stats_res.data_frame.empty
                ):
                    logger.warning(
                        f"Could not retrieve stats for table {table_name}. Query returned no data or an error: {stats_res.error_message if isinstance(stats_res, ErrorResponse) else 'No data'}"
                    )
                    # Add placeholder stats if query fails or returns empty
                    for _, row in group.iterrows():
                        all_stats.append(
                            {
                                "table_name": table_name,
                                "column_name": row["COLUMN_NAME"],
                                "null_percentage": None,
                                "distinct_values_count": None,
                                "most_common_values": [],
                                "most_common_frequencies": [],
                                "minimum_value": None,
                                "maximum_value": None,
                            }
                        )
                    continue

                stats_data = stats_res.data_frame.iloc[0]
                total_rows = stats_data.get("total_rows", 0)

                for _, row in group.iterrows():
                    col = row["COLUMN_NAME"]
                    # Keys for stats_data should match the aliases in stats_query (e.g., "nulls_COLNAME")
                    nulls = stats_data.get(f"nulls_{col}", 0)
                    distincts = stats_data.get(f"distincts_{col}", None)
                    min_val = stats_data.get(f"min_{col}", None)
                    max_val = stats_data.get(f"max_{col}", None)
                    null_pct = (nulls / total_rows) * 100 if total_rows is not None and total_rows > 0 else None

                    all_stats.append(
                        {
                            "table_name": table_name,
                            "column_name": col,
                            "null_percentage": null_pct,
                            "distinct_values_count": distincts,
                            "most_common_values": [],
                            "most_common_frequencies": [],
                            "minimum_value": min_val,
                            "maximum_value": max_val,
                        }
                    )
            except Exception as e:
                logger.error(f"Exception while fetching statistics for table {table_name}: {e}")
                for _, row in group.iterrows():
                    all_stats.append(
                        {
                            "table_name": table_name,
                            "column_name": row["COLUMN_NAME"],
                            "null_percentage": None,
                            "distinct_values_count": None,
                            "most_common_values": [],
                            "most_common_frequencies": [],
                            "minimum_value": None,
                            "maximum_value": None,
                        }
                    )

        if not all_stats:
            return TableResponse(data=pandas.DataFrame())

        return TableResponse(data=pandas.DataFrame(all_stats))

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None) -> DataHandlerResponse:
        """
        Retrieves primary key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve primary key information.

        Returns:
            DataHandlerResponse: A response object containing the primary key information.
        """
        try:
            query = """
                SHOW PRIMARY KEYS IN TABLE;
            """

            response = self.native_query(query)
            if isinstance(response, ErrorResponse):
                logger.error(f"Query error in meta_get_primary_keys: {response.error_message}\nQuery:\n{query}")

            df = response.data_frame
            if not df.empty:
                if table_names:
                    df = df[df["table_name"].isin(table_names)]

                df = df[["table_name", "column_name", "key_sequence", "constraint_name"]]
                df = df.rename(columns={"key_sequence": "ordinal_position"})

            response.data_frame = df

            return response

        except Exception as e:
            logger.error(f"Exception in meta_get_primary_keys: {e!r}")
            return ErrorResponse(error_message=f"Exception querying primary keys: {e!r}")

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None) -> DataHandlerResponse:
        """
        Retrieves foreign key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve foreign key information.

        Returns:
            DataHandlerResponse: A response object containing the foreign key information.
        """
        try:
            query = """
                SHOW IMPORTED KEYS IN TABLE;
            """

            response = self.native_query(query)
            if isinstance(response, ErrorResponse):
                logger.error(f"Query error in meta_get_primary_keys: {response.error_message}\nQuery:\n{query}")

            df = response.data_frame
            if not df.empty:
                if table_names:
                    df = df[df["pk_table_name"].isin(table_names) & df["fk_table_name"].isin(table_names)]

                df = df[["pk_table_name", "pk_column_name", "fk_table_name", "fk_column_name"]]
                df = df.rename(
                    columns={
                        "pk_table_name": "child_table_name",
                        "pk_column_name": "child_column_name",
                        "fk_table_name": "parent_table_name",
                        "fk_column_name": "parent_column_name",
                    }
                )

            response.data_frame = df

            return response

        except Exception as e:
            logger.error(f"Exception in meta_get_primary_keys: {e!r}")
            return ErrorResponse(error_message=f"Exception querying primary keys: {e!r}")

    def meta_get_handler_info(self, **kwargs: Any) -> str:
        """
        Retrieves information about the design and implementation of the database handler.
        This should include, but not be limited to, the following:
        - The type of SQL queries and operations that the handler supports.
        - etc.

        Args:
            kwargs: Additional keyword arguments that may be used in generating the handler information.

        Returns:
            str: A string containing information about the database handler's design and implementation.
        """
        return (
            "To query columns that contain special characters, use ticks around the column name, e.g. `column name`.\n"
            "DO NOT use double quotes for this purpose."
        )
