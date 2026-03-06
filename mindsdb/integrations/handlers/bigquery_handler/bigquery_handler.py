import json
from typing import Any, Dict, Optional, Text

from google.cloud.bigquery import Client, QueryJobConfig, DEFAULT_RETRY
from google.api_core.exceptions import BadRequest, NotFound
import pandas as pd
from sqlalchemy_bigquery.base import BigQueryDialect

from mindsdb.utilities import log
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.utilities.handlers.auth_utilities.google import (
    GoogleServiceAccountOAuth2Manager,
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

logger = log.getLogger(__name__)


class BigQueryHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of Google BigQuery statements.
    """

    name = "bigquery"

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any):
        super().__init__(name)
        self.connection_data = connection_data
        self.client = None
        self.is_connected = False
        self._filtered_tables = None  # Cache for filtered table list

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Establishes a connection to a BigQuery warehouse.

        Raises:
            ValueError: If the required connection parameters are not provided or if the credentials cannot be parsed.
            mindsdb.integrations.utilities.handlers.auth_utilities.exceptions.NoCredentialsException: If none of the required forms of credentials are provided.
            mindsdb.integrations.utilities.handlers.auth_utilities.exceptions.AuthException: If authentication fails.

        Returns:
            google.cloud.bigquery.client.Client: The client object for the BigQuery connection.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters
        if not all(key in self.connection_data for key in ["project_id", "dataset"]):
            raise ValueError("Required parameters (project_id, dataset) must be provided.")

        service_account_json = self.connection_data.get("service_account_json")
        if isinstance(service_account_json, str):
            # GUI send it as str
            try:
                service_account_json = json.loads(service_account_json)
            except json.decoder.JSONDecodeError:
                raise ValueError("'service_account_json' is not valid JSON")
        if isinstance(service_account_json, dict) and isinstance(service_account_json.get("private_key"), str):
            # some editors may escape new line symbol, also replace windows-like newlines
            service_account_json["private_key"] = (
                service_account_json["private_key"].replace("\\n", "\n").replace("\r\n", "\n")
            )

        google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
            credentials_file=self.connection_data.get("service_account_keys"),
            credentials_json=service_account_json,
        )
        credentials = google_sa_oauth2_manager.get_oauth2_credentials()

        client = Client(project=self.connection_data["project_id"], credentials=credentials)
        self.is_connected = True
        self.connection = client
        return self.connection

    def disconnect(self):
        """
        Closes the connection to the BigQuery warehouse if it's currently open.
        Also clears the filtered tables cache.
        """
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        self._filtered_tables = None  # Clear cache on disconnect

    def _parse_table_list(self, table_list_str: Optional[str]) -> list:
        """
        Parse comma-separated table list string into a list of table names.

        Args:
            table_list_str: Comma-separated string of table names or None

        Returns:
            List of table names (empty list if input is None/empty)
        """
        if not table_list_str:
            return []

        # Split by comma, strip whitespace, and filter empty strings
        return [name.strip() for name in table_list_str.split(',') if name.strip()]

    def _get_all_tables_from_dataset(self) -> list:
        """
        Retrieve all table and view names from the configured dataset.

        Returns:
            List of table names in the dataset

        Raises:
            Exception: If query fails or dataset is inaccessible
        """
        query = f"""
            SELECT table_name
            FROM `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
        """

        result = self.native_query(query)

        if result.resp_type != RESPONSE_TYPE.TABLE:
            raise Exception(f"Failed to retrieve tables from dataset: {result.error_message}")

        return result.data_frame['table_name'].tolist()

    def _validate_table_exists(self, table_name: str, available_tables: list) -> bool:
        """
        Validate that a table exists in the dataset.

        Args:
            table_name: Name of the table to validate
            available_tables: List of all available tables in the dataset

        Returns:
            True if table exists, False otherwise
        """
        return table_name in available_tables

    def _get_filtered_tables(self) -> Optional[list]:
        """
        Get filtered table list based on include_tables and exclude_tables parameters.

        This method implements the following logic:
        1. If include_tables is specified:
           - Validate all specified tables exist in the dataset
           - Apply exclude_tables filtering
           - Fail fast if any included table doesn't exist
        2. If only exclude_tables is specified:
           - Discover all tables from dataset
           - Filter out excluded tables
        3. If neither is specified:
           - Return None (signal unfiltered behavior)

        The result is cached in self._filtered_tables for performance.

        Returns:
            List of table names after applying filters, or None if no filtering configured

        Raises:
            ValueError: If include_tables contains non-existent tables
        """
        # Return cached result if available
        if self._filtered_tables is not None:
            return self._filtered_tables

        # Parse configuration parameters
        include_tables_str = self.connection_data.get("include_tables")
        exclude_tables_str = self.connection_data.get("exclude_tables")

        include_tables = self._parse_table_list(include_tables_str)
        exclude_tables = self._parse_table_list(exclude_tables_str)

        # Case 1: No filtering specified - return None to signal unfiltered behavior
        if not include_tables and not exclude_tables:
            logger.info(f"No table filtering configured for dataset {self.connection_data['dataset']}")
            return None

        # Fetch all available tables once for validation
        try:
            available_tables = self._get_all_tables_from_dataset()
        except Exception as e:
            logger.error(f"Failed to retrieve tables for filtering: {e}")
            raise ValueError(f"Cannot apply table filtering: {e}")

        # Case 2: include_tables specified - validate and filter
        if include_tables:
            logger.info(f"Applying include_tables filter: {include_tables}")

            # Validate all specified tables exist (fail-fast)
            missing_tables = [t for t in include_tables if not self._validate_table_exists(t, available_tables)]

            if missing_tables:
                raise ValueError(
                    f"The following tables specified in 'include_tables' do not exist in dataset "
                    f"'{self.connection_data['dataset']}': {', '.join(missing_tables)}. "
                    f"Available tables: {', '.join(available_tables)}"
                )

            # Start with included tables
            filtered_tables = include_tables.copy()

            # Apply exclusions
            if exclude_tables:
                logger.info(f"Applying exclude_tables filter: {exclude_tables}")
                filtered_tables = [t for t in filtered_tables if t not in exclude_tables]

                if not filtered_tables:
                    logger.warning("All included tables were excluded. No tables available.")

            self._filtered_tables = filtered_tables
            logger.info(f"Filtered to {len(filtered_tables)} tables: {filtered_tables}")
            return self._filtered_tables

        # Case 3: Only exclude_tables specified
        if exclude_tables:
            logger.info(f"Applying exclude_tables filter to all tables: {exclude_tables}")

            # Validate excluded tables exist (warning only, not fail-fast)
            invalid_exclusions = [t for t in exclude_tables if not self._validate_table_exists(t, available_tables)]
            if invalid_exclusions:
                logger.warning(
                    f"The following tables in 'exclude_tables' do not exist in dataset "
                    f"'{self.connection_data['dataset']}': {', '.join(invalid_exclusions)}"
                )

            filtered_tables = [t for t in available_tables if t not in exclude_tables]
            self._filtered_tables = filtered_tables
            logger.info(f"Filtered to {len(filtered_tables)} tables after exclusions")
            return self._filtered_tables

        return None  # Should never reach here

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the BigQuery warehouse.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            connection.query("SELECT 1;", timeout=10, retry=DEFAULT_RETRY.with_deadline(10))

            # Check if the dataset exists
            connection.get_dataset(self.connection_data["dataset"])

            response.success = True
        except (BadRequest, ValueError) as e:
            logger.error(f"Error connecting to BigQuery {self.connection_data['project_id']}, {e}!")
            response.error_message = e
        except NotFound:
            response.error_message = (
                f"Error connecting to BigQuery {self.connection_data['project_id']}: "
                f"dataset '{self.connection_data['dataset']}' not found"
            )

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query on the BigQuery warehouse and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        connection = self.connect()
        try:
            job_config = QueryJobConfig(
                default_dataset=f"{self.connection_data['project_id']}.{self.connection_data['dataset']}"
            )
            query = connection.query(query, job_config=job_config)
            result = query.to_dataframe()
            if not result.empty:
                response = Response(RESPONSE_TYPE.TABLE, result)
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Error running query: {query} on {self.connection_data['project_id']}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = SqlalchemyRender(BigQueryDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables and views in the configured dataset.

        Applies include_tables/exclude_tables filtering if configured.

        Returns:
            Response: A response object containing the filtered list of tables and views.
        """
        # Get filtered table list based on configuration
        filtered_tables = self._get_filtered_tables()

        # Build base query
        query = f"""
            SELECT table_name, table_schema, table_type
            FROM `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type IN ('BASE TABLE', 'VIEW')
        """

        # Apply filtering if configured
        if filtered_tables is not None:
            if not filtered_tables:
                # All tables were filtered out - return empty result
                logger.warning("Table filtering resulted in no available tables")
                return Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(columns=['table_name', 'table_schema', 'table_type'])
                )

            # Add WHERE clause for filtered tables
            table_list = ', '.join([f"'{t}'" for t in filtered_tables])
            query += f" AND table_name IN ({table_list})"

        query += " ORDER BY table_name"

        result = self.native_query(query)
        return result

    def get_columns(self, table_name) -> Response:
        """
        Retrieves column details for a specified table in the configured dataset of the BigQuery warehouse.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """
        query = f"""
            SELECT column_name AS Field, data_type as Type
            FROM `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """
        result = self.native_query(query)
        return result

    def meta_get_tables(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves table metadata for the specified tables (or all tables if no list is provided).

        Respects connection-time filtering (include_tables/exclude_tables) and allows
        additional query-time filtering via table_names parameter.

        Args:
            table_names (list): Optional list of table names for query-time filtering.
                               This is intersected with connection-time filters.

        Returns:
            Response: A response object containing the metadata information.
        """
        query = f"""
            SELECT
                t.table_name,
                t.table_schema,
                t.table_type,
                st.row_count
            FROM
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.TABLES` AS t
            JOIN
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.__TABLES__` AS st
            ON
                t.table_name = st.table_id
            WHERE
                t.table_type IN ('BASE TABLE', 'VIEW')
        """

        # Apply connection-time filtering
        filtered_tables = self._get_filtered_tables()

        # Determine final table list (intersection of connection-time and query-time filters)
        final_table_names = None

        if filtered_tables is not None:
            # Connection-time filtering is active
            if table_names is not None and len(table_names) > 0:
                # Query-time filtering also specified - intersect them
                final_table_names = [t for t in table_names if t in filtered_tables]
                logger.debug(
                    f"Intersecting query-time tables {table_names} with connection-time filters: "
                    f"result = {final_table_names}"
                )
            else:
                # Only connection-time filtering
                final_table_names = filtered_tables
        else:
            # No connection-time filtering, use query-time filtering as-is
            final_table_names = table_names

        # Apply final filtering to query
        if final_table_names is not None and len(final_table_names) > 0:
            table_names_quoted = [f"'{t}'" for t in final_table_names]
            query += f" AND t.table_name IN ({','.join(table_names_quoted)})"
        elif final_table_names is not None and len(final_table_names) == 0:
            # Empty filter list - return empty result
            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(columns=['table_name', 'table_schema', 'table_type', 'row_count'])
            )

        result = self.native_query(query)
        return result

    def meta_get_columns(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Respects connection-time filtering (include_tables/exclude_tables) and allows
        additional query-time filtering via table_names parameter.

        Args:
            table_names (list): Optional list of table names for query-time filtering.
                               This is intersected with connection-time filters.

        Returns:
            Response: A response object containing the column metadata.
        """
        query = f"""
            SELECT
                table_name,
                column_name,
                data_type,
                column_default,
                CASE is_nullable
                    WHEN 'YES' THEN TRUE
                    ELSE FALSE
                END AS is_nullable
            FROM
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.COLUMNS`
        """

        # Apply connection-time filtering
        filtered_tables = self._get_filtered_tables()

        # Determine final table list (intersection of connection-time and query-time filters)
        final_table_names = None

        if filtered_tables is not None:
            # Connection-time filtering is active
            if table_names is not None and len(table_names) > 0:
                # Query-time filtering also specified - intersect them
                final_table_names = [t for t in table_names if t in filtered_tables]
            else:
                # Only connection-time filtering
                final_table_names = filtered_tables
        else:
            # No connection-time filtering, use query-time filtering as-is
            final_table_names = table_names

        # Apply final filtering to query
        if final_table_names is not None and len(final_table_names) > 0:
            table_names_quoted = [f"'{t}'" for t in final_table_names]
            query += f" WHERE table_name IN ({','.join(table_names_quoted)})"
        elif final_table_names is not None and len(final_table_names) == 0:
            # Empty filter list - return empty result
            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(columns=['table_name', 'column_name', 'data_type', 'column_default', 'is_nullable'])
            )

        result = self.native_query(query)
        return result

    def meta_get_column_statistics_for_table(self, table_name: str, columns: list) -> Response:
        """
        Retrieves statistics for the specified columns in a table.

        Args:
            table_name (str): The name of the table.
            columns (list): A list of column names to retrieve statistics for.

        Returns:
            Response: A response object containing the column statistics.
        """
        # Check column data types
        column_types_query = f"""
            SELECT column_name, data_type
            FROM `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """
        column_types_result = self.native_query(column_types_query)

        if column_types_result.resp_type != RESPONSE_TYPE.TABLE:
            logger.error(f"Error retrieving column types for table {table_name}")
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Could not retrieve column types for table {table_name}",
            )

        column_type_map = dict(
            zip(
                column_types_result.data_frame["column_name"],
                column_types_result.data_frame["data_type"],
            )
        )

        # Types that don't support MIN/MAX aggregations
        UNSUPPORTED_MINMAX_PREFIXES = ("ARRAY", "STRUCT", "RECORD")
        UNSUPPORTED_MINMAX_TYPES = ("GEOGRAPHY", "JSON", "BYTES")

        def supports_minmax(data_type: str) -> bool:
            """Check if a BigQuery data type supports MIN/MAX operations."""
            if data_type is None:
                return False
            data_type_upper = data_type.upper()
            if any(data_type_upper.startswith(prefix) for prefix in UNSUPPORTED_MINMAX_PREFIXES):
                return False
            if data_type_upper in UNSUPPORTED_MINMAX_TYPES:
                return False
            return True

        # To avoid hitting BigQuery's query size limits, we will chunk the columns into batches.
        BATCH_SIZE = 20

        def chunked(lst, n):
            """Yields successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        queries = []
        for column_batch in chunked(columns, BATCH_SIZE):
            batch_queries = []
            for column in column_batch:
                data_type = column_type_map.get(column)

                if supports_minmax(data_type):
                    # Full statistics for supported types
                    batch_queries.append(
                        f"""
                        SELECT
                            '{table_name}' AS table_name,
                            '{column}' AS column_name,
                            SAFE_DIVIDE(COUNTIF(`{column}` IS NULL), COUNT(*)) * 100 AS null_percentage,
                            CAST(MIN(`{column}`) AS STRING) AS minimum_value,
                            CAST(MAX(`{column}`) AS STRING) AS maximum_value,
                            COUNT(DISTINCT `{column}`) AS distinct_values_count
                        FROM
                            `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.{table_name}`
                        """
                    )
                else:
                    # Limited statistics for complex types (no MIN/MAX/COUNT DISTINCT)
                    logger.info(f"Skipping MIN/MAX for column {column} with unsupported type: {data_type}")
                    batch_queries.append(
                        f"""
                        SELECT
                            '{table_name}' AS table_name,
                            '{column}' AS column_name,
                            SAFE_DIVIDE(COUNTIF(`{column}` IS NULL), COUNT(*)) * 100 AS null_percentage,
                            CAST(NULL AS STRING) AS minimum_value,
                            CAST(NULL AS STRING) AS maximum_value,
                            CAST(NULL AS INT64) AS distinct_values_count
                        FROM
                            `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.{table_name}`
                        """
                    )

            if batch_queries:
                query = " UNION ALL ".join(batch_queries)
                queries.append(query)

        results = []
        for query in queries:
            try:
                result = self.native_query(query)
                if result.resp_type == RESPONSE_TYPE.TABLE:
                    results.append(result.data_frame)
                else:
                    logger.error(f"Error retrieving column statistics for table {table_name}: {result.error_message}")
            except Exception as e:
                logger.error(f"Exception occurred while retrieving column statistics for table {table_name}: {e}")

        if not results:
            logger.warning(f"No column statistics could be retrieved for table {table_name}.")
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"No column statistics could be retrieved for table {table_name}.",
            )
        return Response(
            RESPONSE_TYPE.TABLE,
            pd.concat(results, ignore_index=True) if results else pd.DataFrame(),
        )

    def meta_get_primary_keys(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves primary key information for the specified tables (or all tables if no list is provided).

        Respects connection-time filtering (include_tables/exclude_tables) and allows
        additional query-time filtering via table_names parameter.

        Args:
            table_names (list): Optional list of table names for query-time filtering.
                               This is intersected with connection-time filters.

        Returns:
            Response: A response object containing the primary key information.
        """
        query = f"""
            SELECT
                tc.table_name,
                kcu.column_name,
                kcu.ordinal_position,
                tc.constraint_name,
            FROM
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` AS tc
            JOIN
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` AS kcu
            ON
                tc.constraint_name = kcu.constraint_name
            WHERE
                tc.constraint_type = 'PRIMARY KEY'
        """

        # Apply connection-time filtering
        filtered_tables = self._get_filtered_tables()

        # Determine final table list (intersection of connection-time and query-time filters)
        final_table_names = None

        if filtered_tables is not None:
            # Connection-time filtering is active
            if table_names is not None and len(table_names) > 0:
                # Query-time filtering also specified - intersect them
                final_table_names = [t for t in table_names if t in filtered_tables]
            else:
                # Only connection-time filtering
                final_table_names = filtered_tables
        else:
            # No connection-time filtering, use query-time filtering as-is
            final_table_names = table_names

        # Apply final filtering to query
        if final_table_names is not None and len(final_table_names) > 0:
            table_names_quoted = [f"'{t}'" for t in final_table_names]
            query += f" AND tc.table_name IN ({','.join(table_names_quoted)})"
        elif final_table_names is not None and len(final_table_names) == 0:
            # Empty filter list - return empty result
            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(columns=['table_name', 'column_name', 'ordinal_position', 'constraint_name'])
            )

        result = self.native_query(query)
        return result

    def meta_get_foreign_keys(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves foreign key information for the specified tables (or all tables if no list is provided).

        Respects connection-time filtering (include_tables/exclude_tables) and allows
        additional query-time filtering via table_names parameter.

        Args:
            table_names (list): Optional list of table names for query-time filtering.
                               This is intersected with connection-time filters.

        Returns:
            Response: A response object containing the foreign key information.
        """
        query = f"""
            SELECT
                ccu.table_name AS parent_table_name,
                ccu.column_name AS parent_column_name,
                kcu.table_name AS child_table_name,
                kcu.column_name AS child_column_name,
                tc.constraint_name
            FROM
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` AS tc
            JOIN
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` AS kcu
            ON
                tc.constraint_name = kcu.constraint_name
            JOIN
                `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` AS ccu
            ON
                tc.constraint_name = ccu.constraint_name
            WHERE
                tc.constraint_type = 'FOREIGN KEY'
        """

        # Apply connection-time filtering
        filtered_tables = self._get_filtered_tables()

        # Determine final table list (intersection of connection-time and query-time filters)
        final_table_names = None

        if filtered_tables is not None:
            # Connection-time filtering is active
            if table_names is not None and len(table_names) > 0:
                # Query-time filtering also specified - intersect them
                final_table_names = [t for t in table_names if t in filtered_tables]
            else:
                # Only connection-time filtering
                final_table_names = filtered_tables
        else:
            # No connection-time filtering, use query-time filtering as-is
            final_table_names = table_names

        # Apply final filtering to query
        if final_table_names is not None and len(final_table_names) > 0:
            table_names_quoted = [f"'{t}'" for t in final_table_names]
            query += f" AND tc.table_name IN ({','.join(table_names_quoted)})"
        elif final_table_names is not None and len(final_table_names) == 0:
            # Empty filter list - return empty result
            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(columns=['parent_table_name', 'parent_column_name', 'child_table_name', 'child_column_name', 'constraint_name'])
            )

        result = self.native_query(query)
        return result
