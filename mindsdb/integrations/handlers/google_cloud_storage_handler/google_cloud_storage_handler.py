from typing import Text, Dict, Optional, Any
from google.api_core.exceptions import BadRequest
from google.cloud.storage import Client

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.utilities.handlers.auth_utilities import GoogleServiceAccountOAuth2Manager
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
)

logger = log.getLogger(__name__)


class GoogleCloudStorageHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on Google Cloud Storage.
    """

    name = 'google_cloud_storage'

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs: Any):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Google CLoud Storage bucket.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.client = None
        self.is_connected = False
        self.connection = None

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    @property
    def project_id(self) -> str:
        return self.connection_data.get('project_id')

    @property
    def bucket(self) -> str:
        return self.connection_data.get('bucket')

    @property
    def file_type(self) -> str:
        return self.connection_data.get('file_type')

    @property
    def prefix(self) -> str:
        return self.connection_data.get('prefix')

    @property
    def object_key(self) -> str:
        return self.connection_data.get('object_key')

    def connect(self) -> Client:
        """
        Establishes a connection to Google CLoud Storage.

        Raises: ValueError: If the required connection parameters are not provided or if the credentials cannot be
        parsed. mindsdb.integrations.utilities.handlers.auth_utilities.exceptions.NoCredentialsException: If none of
        the required forms of credentials are provided.
        mindsdb.integrations.utilities.handlers.auth_utilities.exceptions.AuthException: If authentication fails.

        Returns:
            google.cloud.storage.client.Client: The client object for the Google CLoud Storage connection.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters
        if not self.bucket and not self.project_id:
            raise ValueError('Required parameters (project_id, bucket) must be provided.')

        google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
            credentials_file=self.connection_data.get('service_account_keys'),
            credentials_json=self.connection_data.get('service_account_json')
        )
        credentials = google_sa_oauth2_manager.get_oauth2_credentials()

        client = Client(
            project=self.project_id,
            credentials=credentials
        )
        self.is_connected = True
        self.connection = client
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Google Cloud Storage.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            connection.list_buckets()

            # Check if the bucket exists
            connection.get_bucket(self.bucket)

            response.success = True
        except (BadRequest, ValueError) as e:
            logger.error(f'Error connecting to Google CLoud Storage {self.project_id}, {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response
