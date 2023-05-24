from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.gmail_handler.gmail_handler import GmailHandler
from mindsdb.integrations.handlers.gmail_handler.gmail_handler import EmailsTable
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import unittest
from unittest.mock import Mock, MagicMock, patch
from unittest import mock


class GmailHandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.credentials_file = 'test1_credentials.json'
        self.s3_credentials_file = 's3://your-bucket/test_credentials.json'
        self.handler = GmailHandler(connection_data={
            'credentials_file': self.credentials_file,
            's3_credentials_file': self.s3_credentials_file
        })

    @patch('mindsdb.integrations.handlers.gmail_handler.gmail_handler.requests.get')  # Patching the requests.get method
    def test_has_creds_file_with_valid_s3_link(self, mock_get):
        # Configure the mock behavior
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.text = 'Mocked credentials file content'

        result = self.handler._has_creds_file(self.credentials_file)
        # Assert that the requests.get method was called with the correct URL
        mock_get.assert_called_once_with(self.s3_credentials_file)
        # Assert that the method returns True
        self.assertTrue(result)

    @patch('mindsdb.integrations.handlers.gmail_handler.gmail_handler.requests.get')  # Patching the requests.get method
    def test_has_creds_file_with_invalid_s3_link(self, mock_get):
        # Test when invalid S3 credentials file is provided
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        with patch('mindsdb.utilities.log.logger') as mock_logger:
            result = self.handler._has_creds_file(self.credentials_file)
            # Assert that the requests.get method was called with the correct URL
            self.assertFalse(result)
            # Assert that the error message is logged
            mock_logger.error.assert_called_once_with("Failed to get credentials from S3", 404)

    def test_create_connection_with_mocked_token(self):
        with mock.patch('google.oauth2.credentials.Credentials.from_authorized_user_file') as mock_credentials:
            mock_credentials.return_value = Credentials('token.json')
            with mock.patch('os.path.isfile') as mock_isfile:
                mock_isfile.return_value = True
                result = self.handler.create_connection()
                self.assertIsNotNone(result)

    def test_create_connection_with_mocked_credentials_file(self):
        with mock.patch('google.oauth2.credentials.Credentials.from_authorized_user_file') as mock_credentials:
            mock_credentials.is_valid.return_value = False
            with mock.patch('os.path.isfile') as mock_isfile:
                mock_isfile.return_value = True
                result = self.handler.create_connection()
                self.assertIsNotNone(result)

    def test_create_connection_with_mocked_credentials_file_and_s3(self):
        with mock.patch('google.oauth2.credentials.Credentials.from_authorized_user_file') as mock_credentials:
            mock_credentials.is_valid.return_value = False
            with mock.patch('os.path.isfile') as mock_isfile:
                mock_isfile.return_value = True
                with mock.patch(
                        'mindsdb.integrations.handlers.gmail_handler.gmail_handler.GmailHandler._has_creds_file') as mock_has_creds_file:
                    mock_has_creds_file.return_value = True
                    result = self.handler.create_connection()
                    self.assertIsNotNone(result)


class EmailsTableTest(unittest.TestCase):

    def test_get_tables(self):
        handler = Mock(GmailHandler)
        tables = handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_get_columns_returns_all_columns(self):
        gmail_handler = Mock(GmailHandler)
        gmail_table = EmailsTable(gmail_handler)
        expected_columns = [
            'id',
            'message_id',
            'thread_id',
            'label_ids',
            'sender',
            'to',
            'date',
            'subject',
            'snippet',
            'history_id',
            'size_estimate',
            'body',
            'attachments'

        ]
        self.assertListEqual(gmail_table.get_columns(), expected_columns)
