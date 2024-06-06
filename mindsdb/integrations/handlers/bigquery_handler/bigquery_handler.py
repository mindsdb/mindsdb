import os
import json
from google.cloud import bigquery
from typing import Text, Dict, Any
from google.oauth2 import service_account
from sqlalchemy_bigquery.base import BigQueryDialect

from mindsdb.utilities import log
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

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

    def _get_account_keys(self):
        if 'service_account_keys' in self.connection_data:
            if os.path.isfile(self.connection_data['service_account_keys']) is False:
                raise Exception("Service account keys' must be path to the file")
            with open(self.connection_data["service_account_keys"]) as source:
                info = json.load(source)
            return info
        elif 'service_account_json' in self.connection_data:
            info = self.connection_data['service_account_json']
            if not isinstance(info, dict):
                raise Exception("service_account_json has to be dict")
            info['private_key'] = info['private_key'].replace('\\n', '\n')
            return info
        else:
            raise Exception('Connection args have to content ether service_account_json or service_account_keys')

    def connect(self):
        """
        Establishes a connection to a Google BigQuery warehouse.

        Raises:
            ValueError: If the required connection parameter 'project_id' is not provided or if the credentials cannot be parsed.
            mindsdb.integrations.utilities.handlers.auth_utilities.exceptions.AuthException: If none of required forms of credentials are provided.

        Returns:
            google.cloud.bigquery.client.Client: The client object for the BigQuery connection.
        """
        if self.is_connected is True:
            return self.client
        
        # Mandatory connection parameter
        if not 'project_id' in self.connection_data:
            raise ValueError('Required parameter project_id must be provided.')

        info = self._get_account_keys()
        storage_credentials = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(
            project=self.connection_data["project_id"],
            credentials=storage_credentials
        )
        self.is_connected = True
        self.client = client
        return self.client

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
