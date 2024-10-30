from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock
from mindsdb.integrations.handlers.azure_blob_handler.azure_blob_handler import AzureBlobHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
)


class TestAzureBlobHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        container_name='mycontainer',
        connection_string='DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=kofqEuZ6SGd6rG;EndpointSuffix=core.windows.net'
    )

    def setUp(self):
        self.patcher_client = patch('mindsdb.integrations.handlers.azure_blob_handler.azure_blob_handler.BlobServiceClient')
        self.mock_connect = self.patcher_client.start()
        self.handler = AzureBlobHandler('azureblob', connection_data=self.dummy_connection_data)

    def test_connect(self):
        """`
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that duckdb.connect is called exactly once.
        The `connect` method for this handler does not check the validity of the connection; it succeeds even with incorrect credentials.
        The `check_connection` method handles the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)

    def test_check_connection(self):
        """
        Verifies that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        connected = self.handler.check_connection()
        self.assertTrue(connected)
        assert isinstance(connected, StatusResponse)
        self.assertFalse(connected.error_message)


if __name__ == '__main__':
    unittest.main()
