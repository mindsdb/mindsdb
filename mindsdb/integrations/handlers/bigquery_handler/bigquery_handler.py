from typing import Text, Dict, Any
from google.api_core.exceptions import BadRequest
from sqlalchemy_bigquery.base import BigQueryDialect
from google.cloud.bigquery import Client, QueryJobConfig

from mindsdb.utilities import log
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.utilities.handlers.auth_utilities import GoogleServiceAccountOAuth2Manager
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class BigQueryHandler(DatabaseHandler):
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
        if not all(key in self.connection_data for key in ['project_id', 'dataset']):
            raise ValueError('Required parameters (project_id, dataset) must be provided.')

        google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
            credentials_file=self.connection_data.get('service_account_keys'),
            credentials_json=self.connection_data.get('service_account_json')
        )
        credentials = google_sa_oauth2_manager.get_oauth2_credentials()

        client = Client(
            project=self.connection_data["project_id"],
            credentials=credentials
        )
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
            connection.query('SELECT 1;')

            # Check if the dataset exists
            connection.get_dataset(self.connection_data['dataset'])

            response.success = True
        except (BadRequest, ValueError) as e:
            logger.error(f'Error connecting to BigQuery {self.connection_data["project_id"]}, {e}!')
            response.error_message = e

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
            job_config = QueryJobConfig(default_dataset=f"{self.connection_data['project_id']}.{self.connection_data['dataset']}")
            query = connection.query(query, job_config=job_config)
            result = query.to_dataframe()
            if not result.empty:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    result
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f'Error running query: {query} on {self.connection_data["project_id"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
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
            FROM `{self.connection_data['project_id']}.{self.connection_data['dataset']}.INFORMATION_SCHEMA.TABLES`
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
            FROM `{self.connection_data['project_id']}.{self.connection_data['dataset']}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """
        result = self.native_query(query)
        return result
