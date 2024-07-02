import boto3
from botocore.client import BaseClient
from typing import Text, Dict, Optional
from botocore.exceptions import ClientError

from mindsdb.utilities import log

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse

from mindsdb.integrations.handlers.s3_handler.s3_tables import S3BucketsTable, S3ObjectsTable


logger = log.getLogger(__name__)

class S3Handler(APIHandler):
    """
    This handler handles connection and execution of the S3 statements.
    """

    name = 's3'

    def __init__(self, name: Text, connection_data: Optional[Dict] = None, **kwargs: Dict) -> None:
        """
        Initializes the handler and registers API tables.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the AWS (S3) account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        self._register_table("buckets", S3BucketsTable(self))
        self._register_table("objects", S3ObjectsTable(self))

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> BaseClient:
        """
        Establishes a connection to the AWS (S3) account.

        Raises:
            KeyError: If the required connection parameters are not provided.

        Returns:
            botocore.client.BaseClient: A client object to the AWS (S3) account.
        """
        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['aws_access_key_id', 'aws_secret_access_key']):
            raise ValueError('Required parameters (aws_access_key_id, aws_secret_access_key) must be provided.')

        config = {
            'aws_access_key_id': self.connection_data.get('aws_access_key_id'),
            'aws_secret_access_key': self.connection_data.get('aws_secret_access_key')
        }

        # Optional connection parameters.
        optional_params = ['aws_session_token', 'region_name']
        for param in optional_params:
            if param in self.connection_data:
                config[param] = self.connection_data[param]

        try:
            self.connection = boto3.client(
                's3',
                **config
            )
            self.is_connected = True
            return self.connection
        # No exception is raised when invalid credentials are provided at this point.
        # Therefore, it is not possible to catch a more specific exception.
        except Exception as e:
            logger.error(f'Error connecting to AWS, {e}!')
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the AWS (S3) account if it's currently open.
        """
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the AWS (S3) account.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            connection.list_buckets()
            response.success = True
        except ClientError as e:
            logger.error(f'Error connecting to AWS, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response
