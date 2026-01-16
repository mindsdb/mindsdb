from typing import Text, Dict, Any, Optional, List

import pandas as pd
from databricks.sql import connect, RequestError, ServerOperationError
from databricks.sql.client import Connection
from databricks.sqlalchemy import DatabricksDialect
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
    INF_SCHEMA_COLUMNS_NAMES_SET,
)
from mindsdb.utilities import log
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


logger = log.getLogger(__name__)


def _escape_literal(value: str) -> str:
    """Escape a literal string to be safely embedded into SQL single quotes."""
    if not isinstance(value, str):
        raise ValueError("Invalid literal value")
    return value.replace("'", "''")


def _quote_identifier(identifier: str) -> str:
    """Quote identifiers (table/column) for Databricks SQL to avoid injection or syntax errors."""
    if not isinstance(identifier, str) or identifier == "":
        raise ValueError("Invalid identifier value")
    return f"`{identifier.replace('`', '``')}`"


def _validate_identifier(identifier: str) -> str:
    """Validate and sanitize an identifier (table/column name)."""
    import re

    if not isinstance(identifier, str) or not identifier:
        raise ValueError("Identifier must be a non-empty string")
    if not re.match(r"^[\w\s\-]+$", identifier):
        raise ValueError(f"Identifier contains invalid characters: {identifier}")
    return identifier


def _map_type(internal_type_name: str | None) -> MYSQL_DATA_TYPE:
    """Map MyDatabricks SQL text types names to MySQL types as enum.

    Args:
        internal_type_name (str): The name of the Databricks type to map.

    Returns:
        MYSQL_DATA_TYPE: The MySQL type enum that corresponds to the MySQL text type name.
    """
    if not isinstance(internal_type_name, str):
        return MYSQL_DATA_TYPE.TEXT

    type_upper = internal_type_name.upper()

    type_mappings = {
        "STRING": MYSQL_DATA_TYPE.TEXT,
        "LONG": MYSQL_DATA_TYPE.BIGINT,
        "SHORT": MYSQL_DATA_TYPE.SMALLINT,
        "INT": MYSQL_DATA_TYPE.INT,
        "INTEGER": MYSQL_DATA_TYPE.INT,
        "BIGINT": MYSQL_DATA_TYPE.BIGINT,
        "SMALLINT": MYSQL_DATA_TYPE.SMALLINT,
        "TINYINT": MYSQL_DATA_TYPE.TINYINT,
        "FLOAT": MYSQL_DATA_TYPE.FLOAT,
        "DOUBLE": MYSQL_DATA_TYPE.DOUBLE,
        "DECIMAL": MYSQL_DATA_TYPE.DECIMAL,
        "BOOLEAN": MYSQL_DATA_TYPE.BOOL,
        "DATE": MYSQL_DATA_TYPE.DATE,
        "TIMESTAMP": MYSQL_DATA_TYPE.DATETIME,
        "BINARY": MYSQL_DATA_TYPE.BINARY,
    }
    if type_upper in type_mappings:
        return type_mappings[type_upper]

    try:
        return MYSQL_DATA_TYPE(type_upper)
    except Exception:
        logger.info(f"Databricks handler: unknown type: {internal_type_name}, use TEXT as fallback.")
        return MYSQL_DATA_TYPE.TEXT


class DatabricksHandler(MetaDatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on Databricks.
    """

    name = "databricks"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Databricks workspace.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> Connection:
        """
        Establishes a connection to the Databricks workspace.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            databricks.sql.client.Connection: A connection object to the Databricks workspace.
        """
        if self.is_connected is True:
            return self.connection

        if not all(key in self.connection_data for key in ["server_hostname", "http_path", "access_token"]):
            raise ValueError("Required parameters (server_hostname, http_path, access_token) must be provided.")

        config = {
            "server_hostname": self.connection_data["server_hostname"],
            "http_path": self.connection_data["http_path"],
            "access_token": self.connection_data["access_token"],
        }

        optional_parameters = [
            "session_configuration",
            "http_headers",
            "catalog",
            "schema",
        ]
        for parameter in optional_parameters:
            if parameter in self.connection_data:
                config[parameter] = self.connection_data[parameter]

        try:
            self.connection = connect(**config)
            self.is_connected = True
            return self.connection
        except RequestError as request_error:
            logger.error(f"Request error when connecting to Databricks: {request_error}")
            raise
        except RuntimeError as runtime_error:
            logger.error(f"Runtime error when connecting to Databricks: {runtime_error}")
            raise
        except Exception as unknown_error:
            logger.error(f"Unknown error when connecting to Databricks: {unknown_error}")
            raise

    def disconnect(self):
        """
        Closes the connection to the Databricks workspace if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Databricks workspace.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()

            query = "SELECT 1 FROM information_schema.schemata"
            schema_value = self.connection_data.get("schema")
            if isinstance(schema_value, str) and schema_value != "":
                try:
                    _validate_identifier(schema_value)
                    escaped_schema = _escape_literal(schema_value)
                    query += f" WHERE schema_name = '{escaped_schema}'"
                except ValueError as e:
                    logger.error(f"Invalid schema name: {e}")
                    response.error_message = str(e)
                    return response

            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()

            if not result:
                raise ValueError(f"The schema {self.connection_data['schema']} does not exist!")

            response.success = True
        except (
            ValueError,
            RequestError,
            RuntimeError,
            ServerOperationError,
        ) as known_error:
            logger.error(f"Connection check to Databricks failed, {known_error}!")
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f"Connection check to Databricks failed due to an unknown error, {unknown_error}!")
            response.error_message = str(unknown_error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the Databricks workspace and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(result, columns=[x[0] for x in cursor.description]),
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                connection.commit()
            except ServerOperationError as server_error:
                logger.error(f"Server error running query: {query} on Databricks, {server_error}!")
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(server_error), error_code=0)
            except Exception as unknown_error:
                logger.error(f"Unknown error running query: {query} on Databricks, {unknown_error}!")
                response = Response(RESPONSE_TYPE.ERROR, error_message=str(unknown_error), error_code=0)

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Databricks Workspace and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(DatabricksDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self, all: bool = False) -> Response:
        """
        Retrieves a list of all non-system tables in the connected schema of the Databricks workspace.

        Args:
            all (bool): If True - return tables from all schemas.

        Returns:
            Response: A response object containing a list of tables in the connected schema.
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
                table_schema != 'information_schema'
                {all_filter}
        """
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={col: col.upper() for col in df.columns})
        return result

    def get_columns(self, table_name: str, schema_name: str | None = None) -> Response:
        """
        Retrieves column details for a specified table in the Databricks workspace.

        Args:
            table_name (str): The name of the table for which to retrieve column information.
            schema_name (str|None): The name of the schema in which the table is located.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        try:
            _validate_identifier(table_name)
            table_literal = _escape_literal(table_name)
        except ValueError as e:
            logger.error(f"Invalid table name: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        if isinstance(schema_name, str):
            try:
                _validate_identifier(schema_name)
                schema_name_sql = f"'{_escape_literal(schema_name)}'"
            except ValueError as e:
                logger.error(f"Invalid schema name: {e}")
                return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        else:
            schema_name_sql = "current_schema()"

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
                null as CHARACTER_SET_NAME,
                null as COLLATION_NAME
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_literal}'
            AND
                table_schema = {schema_name_sql}
        """

        result = self.native_query(query)
        if result.resp_type == RESPONSE_TYPE.OK:
            result = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame([], columns=list(INF_SCHEMA_COLUMNS_NAMES_SET)),
            )
        result.to_columns_table_response(map_type_fn=_map_type)

        return result

    def meta_get_tables(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves metadata information about the tables in the Databricks database to be stored in the data catalog.

        Args:
            table_names (list): A list of table names for which to retrieve metadata information.

        Returns:
            Response: A response object containing the metadata information, formatted as per the `Response` class.
        """

        schema_name = self.connection_data.get("schema") or "default"

        try:
            _validate_identifier(schema_name)
            schema_literal = _escape_literal(schema_name)
        except ValueError as e:
            logger.error(f"Invalid schema name: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        query = f"""
            SELECT
                table_catalog AS TABLE_CATALOG,
                table_schema AS TABLE_SCHEMA,
                table_name AS TABLE_NAME,
                table_type AS TABLE_TYPE,
                comment AS TABLE_DESCRIPTION,
                NULL AS ROW_COUNT,
                created AS CREATED,
                last_altered AS LAST_ALTERED
            FROM information_schema.tables
            WHERE table_schema = '{schema_literal}'
            AND table_type IN ('BASE TABLE', 'VIEW', 'MANAGED')
        """

        if table_names is not None and len(table_names) > 0:
            try:
                escaped_names = []
                for t in table_names:
                    _validate_identifier(t)
                    escaped_names.append(f"'{_escape_literal(t)}'")
                table_names_str = ", ".join(escaped_names)
                query += f" AND table_name IN ({table_names_str})"
            except ValueError as e:
                logger.error(f"Invalid table name in list: {e}")
                return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        result = self.native_query(query)

        if result.type == RESPONSE_TYPE.TABLE and result.data_frame is not None and not result.data_frame.empty:
            result.data_frame["TABLE_SCHEMA"] = self.name

        return result

    def meta_get_columns(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve column metadata.

        Returns:
            Response: A response object containing the column metadata.
        """

        schema_name = self.connection_data.get("schema") or "default"

        try:
            _validate_identifier(schema_name)
            schema_literal = _escape_literal(schema_name)
        except ValueError as e:
            logger.error(f"Invalid schema name: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        query = f"""
            SELECT
                table_name AS TABLE_NAME,
                column_name AS COLUMN_NAME,
                data_type AS DATA_TYPE,
                comment AS COLUMN_DESCRIPTION,
                column_default AS COLUMN_DEFAULT,
                (is_nullable = 'YES') AS IS_NULLABLE,
                character_maximum_length AS CHARACTER_MAXIMUM_LENGTH,
                character_octet_length AS CHARACTER_OCTET_LENGTH,
                numeric_precision AS NUMERIC_PRECISION,
                numeric_scale AS NUMERIC_SCALE,
                datetime_precision AS DATETIME_PRECISION,
                NULL AS CHARACTER_SET_NAME,
                NULL AS COLLATION_NAME
            FROM information_schema.columns
            WHERE table_schema = '{schema_literal}'
        """

        if table_names is not None and len(table_names) > 0:
            try:
                escaped_names = []
                for t in table_names:
                    _validate_identifier(t)
                    escaped_names.append(f"'{_escape_literal(t.lower())}'")
                table_names_str = ", ".join(escaped_names)
                query += f" AND LOWER(table_name) IN ({table_names_str})"
            except ValueError as e:
                logger.error(f"Invalid table name in list: {e}")
                return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        result = self.native_query(query)
        return result

    def meta_get_column_statistics(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves basic column statistics: null %, distinct count.

        Args:
            table_names (list): A list of table names for which to retrieve column statistics metadata.

        Returns:
            Response: A response object containing the column statistics metadata.
        """
        schema_name = self.connection_data.get("schema") or "default"

        try:
            _validate_identifier(schema_name)
            schema_literal = _escape_literal(schema_name)
        except ValueError as e:
            logger.error(f"Invalid schema name: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        columns_query = f"""
            SELECT table_name AS TABLE_NAME, column_name AS COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = '{schema_literal}'
        """

        if table_names:
            try:
                escaped_names = []
                for t in table_names:
                    _validate_identifier(t)
                    escaped_names.append(f"'{_escape_literal(t)}'")
                table_names_str = ", ".join(escaped_names)
                columns_query += f" AND table_name IN ({table_names_str})"
            except ValueError as e:
                logger.error(f"Invalid table name in list: {e}")
                return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        columns_result = self.native_query(columns_query)
        if (
            columns_result.type == RESPONSE_TYPE.ERROR
            or columns_result.data_frame is None
            or columns_result.data_frame.empty
        ):
            return Response(RESPONSE_TYPE.ERROR, error_message="No columns found.")
        columns_df = columns_result.data_frame
        grouped = columns_df.groupby("TABLE_NAME")
        all_stats = []

        for table_name, group in grouped:
            try:
                _validate_identifier(table_name)
            except ValueError as e:
                logger.warning(f"Skipping invalid table name: {e}")
                continue

            select_parts = []
            for _, row in group.iterrows():
                col = row["COLUMN_NAME"]
                try:
                    _validate_identifier(col)
                except ValueError as e:
                    logger.warning(f"Skipping invalid column name: {e}")
                    continue

                quoted_col = _quote_identifier(col)
                safe_suffix = col.replace("`", "``")
                select_parts.extend(
                    [
                        f"SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS {_quote_identifier(f'nulls_{safe_suffix}')}",
                        f"APPROX_COUNT_DISTINCT({quoted_col}) AS {_quote_identifier(f'distincts_{safe_suffix}')}",
                        f"MIN({quoted_col}) AS {_quote_identifier(f'min_{safe_suffix}')}",
                        f"MAX({quoted_col}) AS {_quote_identifier(f'max_{safe_suffix}')}",
                    ]
                )

            if not select_parts:
                continue

            quoted_table_name = _quote_identifier(table_name)
            stats_query = f"""
            SELECT COUNT(*) AS `total_rows`, {", ".join(select_parts)}
            FROM {quoted_table_name}
            """

            try:
                stats_res = self.native_query(stats_query)
                if stats_res.type != RESPONSE_TYPE.TABLE or stats_res.data_frame is None or stats_res.data_frame.empty:
                    logger.warning(f"Could not retrieve stats for table {table_name}")
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
                    safe_suffix = col.replace("`", "``")
                    nulls = stats_data.get(f"nulls_{safe_suffix}", 0)
                    distincts = stats_data.get(f"distincts_{safe_suffix}", None)
                    min_val = stats_data.get(f"min_{safe_suffix}", None)
                    max_val = stats_data.get(f"max_{safe_suffix}", None)
                    null_pct = (nulls / total_rows) * 100 if total_rows > 0 else None

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
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(all_stats))

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Databricks doesn't have primary key constraints in data warehouses.
        Return empty result like Snowflake does when no keys exist.
        """
        empty_df = pd.DataFrame(
            {
                "table_name": pd.Series([], dtype="object"),
                "column_name": pd.Series([], dtype="object"),
                "ordinal_position": pd.Series([], dtype="Int64"),
                "constraint_name": pd.Series([], dtype="object"),
            }
        )

        return Response(RESPONSE_TYPE.TABLE, data_frame=empty_df)

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Databricks doesn't have foreign key constraints in data warehouses.
        Return empty result like Snowflake does when no keys exist.
        """
        empty_df = pd.DataFrame(
            {
                "child_table_name": pd.Series([], dtype="object"),
                "child_column_name": pd.Series([], dtype="object"),
                "parent_table_name": pd.Series([], dtype="object"),
                "parent_column_name": pd.Series([], dtype="object"),
            }
        )

        return Response(RESPONSE_TYPE.TABLE, data_frame=empty_df)
