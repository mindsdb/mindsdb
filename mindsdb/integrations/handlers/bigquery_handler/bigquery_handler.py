from google.cloud import bigquery
from typing import Text, Dict, Any
from google.oauth2 import service_account
from sqlalchemy_bigquery.base import BigQueryDialect

from mindsdb.utilities import log
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
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
            ValueError: If the required connection parameter 'project_id' is not provided or if the credentials cannot be parsed.
            mindsdb.integrations.utilities.handlers.auth_utilities.exceptions.NoCredentialsException: If none of the required forms of credentials are provided.
            mindsdb.integrations.utilities.handlers.auth_utilities.exceptions.AuthException: If authentication fails.

        Returns:
            google.cloud.bigquery.client.Client: The client object for the BigQuery connection.
        """
        if self.is_connected is True:
            return self.client
        
        # Mandatory connection parameter
        if not 'project_id' in self.connection_data:
            raise ValueError('Required parameter project_id must be provided.')

        google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
            credentials_url=self.connection_data.get('service_account_keys'),
            credentials_json=self.connection_data.get('service_account_json')
        )
        credentials = google_sa_oauth2_manager.get_oauth2_credentials()

        client = bigquery.Client(
            project=self.connection_data["project_id"],
            credentials=credentials
        )
        self.is_connected = True
        self.client = client
        return self.client
    
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
        Check the connection of the BigQuery
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)

        try:
            client = self.connect()
            client.query('SELECT 1;')

            # If a dataset is provided, check if it exists
            if 'dataset' in self.connection_data:
                client.get_dataset(self.connection_data['dataset'])

            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to BigQuery {self.connection_data["project_id"]}, {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in BigQuery
        :return: returns the records from the current recordset
        """
        client = self.connect()
        try:
            if 'dataset' in self.connection_data:
                job_config = bigquery.QueryJobConfig(default_dataset=f"{self.connection_data['project_id']}.{self.connection_data['dataset']}")
                query = client.query(query, job_config=job_config)
            else:
                query = client.query(query)
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
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        renderer = SqlalchemyRender(BigQueryDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in BigQuery
        """
        q = f"SELECT table_name, table_type, FROM \
             `{self.connection_data['project_id']}.{self.connection_data['dataset']}.INFORMATION_SCHEMA.TABLES`"
        result = self.native_query(q)
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"SELECT column_name AS Field, data_type as Type, FROM \
            `{self.connection_data['project_id']}.{self.connection_data['dataset']}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}'"
        result = self.native_query(q)
        return result
