import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from duckdb import DuckDBPyConnection
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from datetime import datetime, timedelta

logger = log.getLogger(__name__)

class AzureBlobHandler(APIHandler):
    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        """ constructor
        Args:
            name (str): the handler name
        """

        self._tables = {}
        self.storage_account_name = None
        self.account_access_key = None
        self.is_connected = False

        connection_data = kwargs.get('connection_data')
        if 'storage_account_name' in connection_data:
            self.storage_account_name = connection_data['storage_account_name']
        if 'account_access_key' in connection_data:
            self.account_access_key = connection_data['account_access_key']
        

    def connect(self) -> BlobServiceClient:
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        sas_token = generate_account_sas(
            account_name=self.storage_account_name,
            account_key=self.account_access_key,
            resource_types=ResourceTypes(service=True),
            permission=AccountSasPermissions(read=True),
            expiry=datetime.now() + timedelta(hours=1)
        )

        blob_service_client = BlobServiceClient(
            account_url=f"https://{self.storage_account_name}.blob.core.windows.net",
            credential=sas_token
        )

        return blob_service_client

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            client = self.connect()
            client.get_account_information()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Azure Blob: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        """

    def call_application_api(self, method_name:str = None, params:dict = None) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. Can be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            DataFrame
        """

