import unittest
from botocore.client import ClientError
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.handlers.s3_handler.s3_handler import S3Handler


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestS3Handler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        aws_access_key_id='AQAXEQK89OX07YS34OP',
        aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        bucket='mindsdb-bucket',
        region_name='us-east-2',
    )

    def setUp(self):
        self.patcher = patch('duckdb.connect')
        self.mock_connect = self.patcher.start()
        self.handler = S3Handler('s3', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that duckdb.connect is called exactly once.
        The `connect` method for this handler does not check the validity of the connection; it succeeds even with incorrect credentials.
        The `check_connection` method handles the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    @patch('boto3.client')
    def test_check_connection_success(self, mock_boto3_client):
        """
        Verifies that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance
        mock_boto3_client_instance.head_bucket.return_value = {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-bucket-region': 'us-east-2',
                }
            }
        }
        mock_boto3_client_instance.meta = MagicMock(region_name='us-east-2')
    
        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    @patch('boto3.client')
    def test_check_connection_failure_invalid_bucket_or_no_access(self, mock_boto3_client):
        """
        Verifies that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance
        mock_boto3_client_instance.head_bucket.side_effect = ClientError(
            error_response={
                'Error': {
                    'Code': '404',
                    'Message': 'Not Found',
                }
            },
            operation_name='HeadBucket'
        )
    
        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    @patch('boto3.client')
    def test_check_connection_failure_invalid_bucket_region(self, mock_boto3_client):
        """
        Verifies that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance
        mock_boto3_client_instance.head_bucket.return_value = {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-bucket-region': 'us-east-2',
                }
            }
        }
        mock_boto3_client_instance.meta = MagicMock(region_name='us-east-1')
    
        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)


if __name__ == '__main__':
    unittest.main()
