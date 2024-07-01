import boto3
from typing import Optional

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

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
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

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = boto3.client(
            's3',
            aws_access_key_id=self.connection_data['aws_access_key_id'],
            aws_secret_access_key=self.connection_data['aws_secret_access_key'],
            region_name=self.connection_data['region_name']
        )
        self.is_connected = True

        return self.connection

    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            connection.head_object(Bucket=self.connection_data['bucket'], Key=self.connection_data['key'])
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to AWS with the given credentials, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

