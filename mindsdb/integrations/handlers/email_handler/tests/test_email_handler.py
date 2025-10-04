# mindsdb/integrations/handlers/email_handler/tests/test_email_handler.py

import imaplib
import unittest
from unittest.mock import MagicMock, patch

from mindsdb.integrations.handlers.email_handler.email_handler import EmailHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse


class EmailHandlerTest(unittest.TestCase):
    @patch("mindsdb.integrations.handlers.email_handler.email_handler.imaplib")
    def test_connect_with_advanced_settings(self, mock_imaplib):
        mock_connection = MagicMock()
        mock_connection.login.return_value = ("OK", [b"Login successful"])
        mock_imaplib.IMAP4_SSL.return_value = mock_connection
        connection_data = {
            "email": "user@proton.me",
            "password": "test_password",
            "host": "127.0.0.1",
            "port": 1143,
            "username": "user@localhost",
        }
        handler = EmailHandler("test_email_handler", connection_data=connection_data)
        handler.connect()
        mock_imaplib.IMAP4_SSL.assert_called_once_with("127.0.0.1", 1143)
        mock_connection.login.assert_called_once_with("user@localhost", "test_password")

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.imaplib")
    def test_connect_fallback_to_deduced_settings(self, mock_imaplib):
        mock_connection = MagicMock()
        mock_connection.login.return_value = ("OK", [b"Login successful"])
        mock_imaplib.IMAP4_SSL.return_value = mock_connection
        connection_data = {"email": "user@example.com", "password": "test_password"}
        handler = EmailHandler("test_email_handler", connection_data=connection_data)
        handler.connect()
        mock_imaplib.IMAP4_SSL.assert_called_once_with("imap.example.com", 993)
        mock_connection.login.assert_called_once_with("user@example.com", "test_password")

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.imaplib")
    def test_connect_no_ssl(self, mock_imaplib):
        mock_connection = MagicMock()
        mock_connection.login.return_value = ("OK", [b"Login successful"])
        mock_imaplib.IMAP4.return_value = mock_connection
        connection_data = {
            "email": "user@insecure.com",
            "password": "test_password",
            "host": "imap.insecure.com",
            "port": 143,
            "use_ssl": False,
        }
        handler = EmailHandler("test_email_handler", connection_data=connection_data)
        handler.connect()
        mock_imaplib.IMAP4_SSL.assert_not_called()
        mock_imaplib.IMAP4.assert_called_once_with("imap.insecure.com", 143)
        mock_connection.login.assert_called_once_with("user@insecure.com", "test_password")

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.imaplib")
    def test_check_connection_success(self, mock_imaplib):
        mock_connection = MagicMock()
        mock_connection.login.return_value = ("OK", [b"Login successful"])
        mock_imaplib.IMAP4_SSL.return_value = mock_connection
        connection_data = {"email": "user@example.com", "password": "pwd"}
        handler = EmailHandler("test_email_handler", connection_data=connection_data)
        status = handler.check_connection()
        self.assertIsInstance(status, HandlerStatusResponse)
        self.assertTrue(status.success)
        mock_connection.logout.assert_called_once()

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.imaplib")
    def test_check_connection_failure(self, mock_imaplib):
        mock_imaplib.IMAP4_SSL.side_effect = imaplib.IMAP4.error("Authentication failed")
        connection_data = {
            "email": "user@example.com",
            "password": "wrong_password",
        }
        handler = EmailHandler("test_email_handler", connection_data=connection_data)
        status = handler.check_connection()
        self.assertIsInstance(status, HandlerStatusResponse)
        self.assertFalse(status.success)
        self.assertIn("Authentication failed", status.error_message)


if __name__ == "__main__":
    unittest.main()
