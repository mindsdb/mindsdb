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
from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleServiceAccountOAuth2Manager
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
        """
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False

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
        Retrieves a list of all non-system tables and views in the configured dataset of the BigQuery warehouse.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        query = f"""
            SELECT table_name, table_schema, table_type
            FROM `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type IN ('BASE TABLE', 'VIEW')
        """
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

        Args:
            table_names (list): A list of table names for which to retrieve metadata information.

        Returns:
            Response: A response object containing the metadata information, formatted as per the `Response` class.
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

        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t}'" for t in table_names]
            query += f" WHERE table_name IN ({','.join(table_names)})"

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
        # To avoid hitting BigQuery's query size limits, we will chunk the columns into batches.
        # This is because the queries are combined using UNION ALL, which can lead to very large queries if there are many columns.
        BATCH_SIZE = 20

        def chunked(lst, n):
            """
            Yields successive n-sized chunks from lst.
            """
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        queries = []
        for column_batch in chunked(columns, BATCH_SIZE):
            batch_queries = []
            for column in column_batch:
                batch_queries.append(
                    f"""
                    SELECT
                        '{table_name}' AS table_name,
                        '{column}' AS column_name,
                        SAFE_DIVIDE(COUNTIF({column} IS NULL), COUNT(*)) * 100 AS null_percentage,
                        CAST(MIN(`{column}`) AS STRING) AS minimum_value,
                        CAST(MAX(`{column}`) AS STRING) AS maximum_value,
                        COUNT(DISTINCT {column}) AS distinct_values_count
                    FROM
                        `{self.connection_data["project_id"]}.{self.connection_data["dataset"]}.{table_name}`
                    """
                )

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
                RESPONSE_TYPE.ERROR, error_message=f"No column statistics could be retrieved for table {table_name}."
            )
        return Response(RESPONSE_TYPE.TABLE, pd.concat(results, ignore_index=True) if results else pd.DataFrame())

    def meta_get_primary_keys(self, table_names: Optional[list] = None) -> Response:
        """
        Retrieves primary key information for the specified tables (or all tables if no list is provided).

        Args:
            table_names (list): A list of table names for which to retrieve primary key information.

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

        if table_names is not None and len(table_names) > 0:
            table_names = [f"'{t}'" for t in table_names]
            query += f" AND tc.table_name IN ({','.join(table_names)})"

        result = self.native_query(query)
        return result
