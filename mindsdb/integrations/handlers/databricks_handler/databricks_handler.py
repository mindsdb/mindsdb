from typing import Text, Dict, Any, Optional, List

from databricks.sql import connect, RequestError, ServerOperationError
from databricks.sql.client import Connection
from databricks.sqlalchemy import DatabricksDialect
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
import pandas as pd

from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


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

        # Mandatory connection parameters.
        if not all(
            key in self.connection_data
            for key in ["server_hostname", "http_path", "access_token"]
        ):
            raise ValueError('Required parameters (server_hostname, http_path, access_token) must be provided.')

        config = {
            "server_hostname": self.connection_data["server_hostname"],
            "http_path": self.connection_data["http_path"],
            "access_token": self.connection_data["access_token"],
        }

        # Optional connection parameters.
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
            self.connection = connect(
                **config
            )
            self.is_connected = True
            return self.connection
        except RequestError as request_error:
            logger.error(f'Request error when connecting to Databricks: {request_error}')
            raise
        except RuntimeError as runtime_error:
            logger.error(f'Runtime error when connecting to Databricks: {runtime_error}')
            raise
        except Exception as unknown_error:
            logger.error(f'Unknown error when connecting to Databricks: {unknown_error}')
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

            # Execute a simple query to check the connection.
            query = "SELECT 1 FROM information_schema.schemata"
            if 'schema' in self.connection_data:
                query += f" WHERE schema_name = '{self.connection_data['schema']}'"

            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()

            # If the query does not return a result, the schema does not exist.
            if not result:
                raise ValueError(f'The schema {self.connection_data["schema"]} does not exist!')

            response.success = True
        except (ValueError, RequestError, RuntimeError, ServerOperationError) as known_error:
            logger.error(f'Connection check to Databricks failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Databricks failed due to an unknown error, {unknown_error}!')
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
                        data_frame=pd.DataFrame(
                            result, columns=[x[0] for x in cursor.description]
                        ),
                    )

                else:
                    response = Response(RESPONSE_TYPE.OK)
                    connection.commit()
            except ServerOperationError as server_error:
                logger.error(
                    f'Server error running query: {query} on Databricks, {server_error}!'
                )
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(server_error)
                )
            except Exception as unknown_error:
                logger.error(
                    f'Unknown error running query: {query} on Databricks, {unknown_error}!'
                )
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(unknown_error)
                )

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

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables in the connected schema of the Databricks workspace.

        Returns:
            Response: A response object containing a list of tables in the connected schema.
        """
        query = """
            SHOW TABLES;
        """
        result = self.native_query(query)

        df = result.data_frame
        if df is not None:
            result.data_frame = df.rename(columns={"tableName": "table_name", "database": "schema_name"})
        return result

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table in the Databricks workspace.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        query = f"DESCRIBE TABLE {table_name};"
        result = self.native_query(query)

        df = result.data_frame
        result.data_frame = df.rename(columns={"col_name": "column_name"})
        return result

    def meta_get_tables(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves metadata information about the tables in the Databricks database to be stored in the data catalog.

        Args:
            table_names (list): A list of table names for which to retrieve metadata information.

        Returns:
            Response: A response object containing the metadata information, formatted as per the `Response` class.
        """
        
        schema_name = self.connection_data.get('schema', 'default')
        
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
            WHERE table_schema = '{schema_name}'
            AND table_type IN ('BASE TABLE', 'VIEW', 'MANAGED')
        """

        if table_names is not None and len(table_names) > 0:
            table_names_str = ", ".join([f"'{t}'" for t in table_names])
            query += f" AND table_name IN ({table_names_str})"

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
        
        # Get the schema from connection data or use 'default'
        schema_name = self.connection_data.get('schema', 'default')
        
        # Use only columns that exist in Databricks information_schema.columns
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
            WHERE table_schema = '{schema_name}'
        """

        if table_names is not None and len(table_names) > 0:
            table_names_str = ", ".join([f"'{t.lower()}'" for t in table_names])
            query += f" AND LOWER(table_name) IN ({table_names_str})"

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
        # Get the schema from connection data or use 'default'
        schema_name = self.connection_data.get('schema', 'default')
        
        columns_query = f"""
            SELECT table_name AS TABLE_NAME, column_name AS COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
        """
        if table_names:
            table_names_str = ", ".join([f"'{t}'" for t in table_names])
            columns_query += f" AND table_name IN ({table_names_str})"

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
            select_parts = []
            for _, row in group.iterrows():
                col = row["COLUMN_NAME"]
                quoted_col = f'`{col}`'
                select_parts.extend([
                    f'SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS `nulls_{col}`',
                    f'APPROX_COUNT_DISTINCT({quoted_col}) AS `distincts_{col}`',
                    f'MIN({quoted_col}) AS `min_{col}`',
                    f'MAX({quoted_col}) AS `max_{col}`',
                ])

            quoted_table_name = f'`{table_name}`'
            stats_query = f"""
            SELECT COUNT(*) AS `total_rows`, {", ".join(select_parts)}
            FROM {quoted_table_name}
            """
            
            try:
                stats_res = self.native_query(stats_query)
                if stats_res.type != RESPONSE_TYPE.TABLE or stats_res.data_frame is None or stats_res.data_frame.empty:
                    logger.warning(f"Could not retrieve stats for table {table_name}")
                    # Add placeholder stats if query fails
                    for _, row in group.iterrows():
                        all_stats.append({
                            "table_name": table_name,
                            "column_name": row["COLUMN_NAME"],
                            "null_percentage": None,
                            "distinct_values_count": None,
                            "most_common_values": [],
                            "most_common_frequencies": [],
                            "minimum_value": None,
                            "maximum_value": None,
                        })
                    continue

                stats_data = stats_res.data_frame.iloc[0]
                total_rows = stats_data.get("total_rows", 0)

                for _, row in group.iterrows():
                    col = row["COLUMN_NAME"]
                    nulls = stats_data.get(f"nulls_{col}", 0)
                    distincts = stats_data.get(f"distincts_{col}", None)
                    min_val = stats_data.get(f"min_{col}", None)
                    max_val = stats_data.get(f"max_{col}", None)
                    null_pct = (nulls / total_rows) * 100 if total_rows > 0 else None

                    all_stats.append({
                        "table_name": table_name,
                        "column_name": col,
                        "null_percentage": null_pct,
                        "distinct_values_count": distincts,
                        "most_common_values": [],
                        "most_common_frequencies": [],
                        "minimum_value": min_val,
                        "maximum_value": max_val,
                    })
            except Exception as e:
                logger.error(f"Exception while fetching statistics for table {table_name}: {e}")
                for _, row in group.iterrows():
                    all_stats.append({
                        "table_name": table_name,
                        "column_name": row["COLUMN_NAME"],
                        "null_percentage": None,
                        "distinct_values_count": None,
                        "most_common_values": [],
                        "most_common_frequencies": [],
                        "minimum_value": None,
                        "maximum_value": None,
                    })
        if not all_stats:
            return Response(RESPONSE_TYPE.TABLE, data_frame=pandas.DataFrame())

        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(all_stats))

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Databricks doesn't have primary key constraints in data warehouses.
        Return empty result like Snowflake does when no keys exist.
        """
        empty_df = pd.DataFrame({
            'table_name': pd.Series([], dtype='object'),
            'column_name': pd.Series([], dtype='object'),
            'ordinal_position': pd.Series([], dtype='Int64'),
            'constraint_name': pd.Series([], dtype='object')
        })
        
        return Response(RESPONSE_TYPE.TABLE, data_frame=empty_df)

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Databricks doesn't have foreign key constraints in data warehouses.
        Return empty result like Snowflake does when no keys exist.
        """
        empty_df = pd.DataFrame({
            'child_table_name': pd.Series([], dtype='object'),
            'child_column_name': pd.Series([], dtype='object'),
            'parent_table_name': pd.Series([], dtype='object'),
            'parent_column_name': pd.Series([], dtype='object')
        })
        
        return Response(RESPONSE_TYPE.TABLE, data_frame=empty_df)