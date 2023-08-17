from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.gmail_handler.gmail_handler import GmailHandler
from mindsdb.integrations.handlers.gmail_handler.gmail_handler import EmailsTable
from google.oauth2.credentials import Credentials
from mindsdb_sql import parse_sql
import unittest
from unittest.mock import Mock, patch
from unittest import mock
import logging


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

        with patch('mindsdb.integrations.handlers.gmail_handler.gmail_handler.logger') as mock_logger:
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
                        'mindsdb.integrations.handlers.gmail_handler.gmail_handler.GmailHandler._has_creds_file') \
                        as mock_has_creds_file:
                    mock_has_creds_file.return_value = True
                    result = self.handler.create_connection()
                    self.assertIsNotNone(result)

    def test_parse_parts_with_multipart_mime_type(self):
        email_parts = [
            {
                'mimeType': 'multipart/mixed',
                'parts': [
                    {
                        'mimeType': 'multipart/alternative',
                        'parts': [
                            {
                                'mimeType': 'text/plain',
                                'body': {
                                    'data': 'VGhpcyBpcyB0aGUgcGxhaW4gdGV4dCBib2R5IG9mIHRoZSBlbWFpbC4='
                                }
                            },
                            {
                                'mimeType': 'text/html',
                                'body': {
                                    'data': 'PGh0bWw+CiAgICA8Ym9keT4KICAgICAgPHA+V'
                                            'GhpcyBpcyB0aGUgSFRNTCBib2R5IG9mIHRoZSBlbWFpbC4'
                                            'gPC9wPgogICAgPC9ib2R5PjwvaHRtbD4='
                                }
                            }
                        ]
                    },
                    {
                        'mimeType': 'application/pdf',
                        'filename': 'example.pdf',
                        'body': {
                            'attachmentId': '<<attachment_id>>'
                        }
                    }
                ]
            }

        ]
        attachments = []
        email_body = self.handler._parse_parts(email_parts, attachments)
        expected_body = "This is the plain text body of the email."
        expected_attachments = [
            {
                'filename': 'example.pdf',
                'mimeType': 'application/pdf',
                'attachmentId': '<<attachment_id>>'
            }
        ]
        self.assertEqual(email_body, expected_body)
        self.assertEqual(attachments, expected_attachments)

    def test_parse_parts_with_multipart_mime_type_and_no_parts(self):
        email_parts = [
            {
                'mimeType': 'multipart/mixed',
                'parts': []
            }
        ]
        attachments = []
        email_body = self.handler._parse_parts(email_parts, attachments)
        expected_body = ""
        expected_attachments = []
        self.assertEqual(email_body, expected_body)
        self.assertEqual(attachments, expected_attachments)

    def test_parse_parts_with_multiple_attachments(self):
        email_parts = [
            {
                'mimeType': 'multipart/mixed',
                'parts': [
                    {
                        'mimeType': 'multipart/alternative',
                        'parts': [
                            {
                                'mimeType': 'text/plain',
                                'body': {
                                    'data': 'VGhpcyBpcyB0aGUgcGxhaW4gdGV4dCBib2R5IG9mIHRoZSBlbWFpbC4='
                                }
                            },
                            {
                                'mimeType': 'text/html',
                                'body': {
                                    'data': 'PGh0bWw+CiAgICA8Ym9keT4KICAgICAgPHA+'
                                            'VGhpcyBpcyB0aGUgSFRNTCBib2R5IG9mIHRoZSBlbWFpb'
                                            'C4gPC9wPgogICAgPC9ib2R5PjwvaHRtbD4='
                                }
                            }
                        ]
                    },
                    {
                        'mimeType': 'application/pdf',
                        'filename': 'example.pdf',
                        'body': {
                            'attachmentId': '<<attachment_id>>'
                        }
                    },
                    {
                        'mimeType': 'application/pdf',
                        'filename': 'example2.pdf',
                        'body': {
                            'attachmentId': '<<attachment_id2>>'
                        }
                    }
                ]
            }
        ]
        attachments = []
        email_body = self.handler._parse_parts(email_parts, attachments)
        expected_body = "This is the plain text body of the email."
        expected_attachments = [
            {
                'filename': 'example.pdf',
                'mimeType': 'application/pdf',
                'attachmentId': '<<attachment_id>>'
            },
            {
                'filename': 'example2.pdf',
                'mimeType': 'application/pdf',
                'attachmentId': '<<attachment_id2>>'
            }
        ]
        self.assertEqual(email_body, expected_body)
        self.assertEqual(attachments, expected_attachments)


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

    def test_delete_method(self):
        gmail_handler = Mock(GmailHandler)
        gmail_table = EmailsTable(gmail_handler)
        query = parse_sql('delete from gmail where id=1', dialect='mindsdb')
        gmail_table.delete(query)
        gmail_handler.call_gmail_api.assert_called_once_with('delete_message', {'id': 1})

    def test_update_method(self):
        gmail_handler = Mock(GmailHandler)
        gmail_table = EmailsTable(gmail_handler)
        query = parse_sql('update gmail set addLabel="test1",removeLabel = "test" where id=1', dialect='mindsdb')
        gmail_table.update(query)
        gmail_handler.call_gmail_api.assert_called_once_with('modify_message', {'id': 1,
                                                                                'body': {'addLabelIds': ['test1'],
                                                                                         'removeLabelIds': ['test']}})
