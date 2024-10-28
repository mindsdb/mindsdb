from typing import Optional, Dict
import ibm_boto3
from ibm_botocore.client import Config, ClientError
from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APIHandler, APIResource
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

logger = log.getLogger(__name__)


class IBMCloudObjectStorageHandler(APIHandler):
    """
    This handler manages the connection and operations with IBM Cloud Object Storage.
    """

    name = "ibm_cos"
    supported_file_formats = ["csv", "tsv", "json", "parquet"]

    def __init__(self, name: str, connection_data: Optional[Dict] = None, **kwargs):
        """
        Initializes the handler.

        Args:
            name (str): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the IBM COS account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data or {}
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True
        self._buckets = None
        self._regions = {}

    def connect(self) -> StatusResponse:
        """
        Establishes a connection to IBM COS.

        Returns:
            StatusResponse: The status of the connection attempt.
        """

        logger.info("Connecting!!")
        if self.is_connected:
            return StatusResponse(True)

        try:
            print("IBM BOTO3 CLIENT YE")
            logger.info("IBM BOTO3 CLIENT YE")
            self.connection = ibm_boto3.client(
                "s3",
                ibm_api_key_id=self.connection_data["cos_api_key_id"],
                ibm_service_instance_id=self.connection_data["cos_service_instance_id"],
                config=Config(signature_version="oauth"),
                endpoint_url=self.connection_data["cos_endpoint_url"],
            )
            logger.info(f"SELF.CONNECTION: {self.connection}")
            self._buckets = self.connection.list_buckets()
            logger.info(f"LIST OF BUCKETS: {self._buckets}")
            self.is_connected = True
            return StatusResponse(True)
        except ClientError as e:
            logger.error(f"ClientError while connecting to IBM COS: {e}")
            return StatusResponse(False, str(e))
        except Exception as e:
            logger.error(f"Error while connecting to IBM COS: {e}")
            return StatusResponse(False, str(e))

    def disconnect(self):
        """
        Closes the connection to IBM COS.
        """
        if not self.is_connected:
            return
        self.connection = None
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks if the connection to IBM COS is alive.

        Returns:
            StatusResponse: The status of the connection.
        """
        if not self.is_connected:
            return StatusResponse(False, "Not connected.")
        logger.info("Check connection")
        try:
            self.connection.list_buckets()
            logger.info(self.connection.list_buckets())
            return StatusResponse(True)
        except ClientError as e:
            logger.error(f"ClientError during connection check: {e}")
            return StatusResponse(False, str(e))
        except Exception as e:
            logger.error(f"Error during connection check: {e}")
            return StatusResponse(False, str(e))
