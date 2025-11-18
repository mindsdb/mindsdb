import pytest
from unittest.mock import MagicMock, patch, Mock
import pandas as pd
import imaplib
import smtplib

from mindsdb.integrations.handlers.email_handler.email_client import EmailClient
from mindsdb.integrations.handlers.email_handler.email_handler import EmailHandler
from mindsdb.integrations.handlers.email_handler.settings import (
    EmailConnectionDetails,
    EmailSearchOptions,
)


class TestEmailClientUnit:
    """Unit tests for EmailClient without real connections"""

    @patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL")
    @patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP")
    def test_init(self, mock_smtp, mock_imap):
        """Test EmailClient initialization"""
        connection_data = EmailConnectionDetails(
            email="test@example.com", password="password123"
        )
        client = EmailClient(connection_data)

        assert client.email == "test@example.com"
        assert client.password == "password123"
        mock_imap.assert_called_once()
        mock_smtp.assert_called_once()

    @patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL")
    @patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP")
    def test_select_mailbox_success(self, mock_smtp, mock_imap):
        """Test successful mailbox selection"""
        mock_imap_instance = MagicMock()
        mock_imap.return_value = mock_imap_instance
        mock_imap_instance.login.return_value = ("OK", [])
        mock_imap_instance.select.return_value = ("OK", [])

        connection_data = EmailConnectionDetails(
            email="test@example.com", password="password123"
        )
        client = EmailClient(connection_data)
        client.select_mailbox("INBOX")

        mock_imap_instance.login.assert_called_once()
        mock_imap_instance.select.assert_called_once_with("INBOX")

    @patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL")
    @patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP")
    def test_select_mailbox_login_failure(self, mock_smtp, mock_imap):
        """Test mailbox selection with login failure"""
        mock_imap_instance = MagicMock()
        mock_imap.return_value = mock_imap_instance
        mock_imap_instance.login.return_value = ("NO", ["Authentication failed"])

        connection_data = EmailConnectionDetails(
            email="test@example.com", password="wrong_password"
        )
        client = EmailClient(connection_data)

        with pytest.raises(ValueError, match="Unable to login"):
            client.select_mailbox("INBOX")

    @patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL")
    @patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP")
    def test_send_email_authentication_error(self, mock_smtp, mock_imap):
        """Test send_email with authentication error"""
        mock_smtp_instance = MagicMock()
        mock_smtp.return_value = mock_smtp_instance
        mock_smtp_instance.login.side_effect = smtplib.SMTPAuthenticationError(
            535, "Authentication failed"
        )
        connection_data = EmailConnectionDetails(
            email="test@example.com", password="wrong_password"
        )
        client = EmailClient(connection_data)

        with pytest.raises(ValueError, match="Failed to send email to recipient@example.com: \\(535, 'Authentication failed'\\)"):
            client.send_email("recipient@example.com", "Test", "Body")

    @patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL")
    @patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP")
    def test_logout_graceful(self, mock_smtp, mock_imap):
        """Test graceful logout"""
        mock_imap_instance = MagicMock()
        mock_smtp_instance = MagicMock()
        mock_imap.return_value = mock_imap_instance
        mock_smtp.return_value = mock_smtp_instance

        mock_imap_instance.logout.return_value = ("BYE", [])

        connection_data = EmailConnectionDetails(
            email="test@example.com", password="password123"
        )
        client = EmailClient(connection_data)
        client.logout()

        mock_imap_instance.logout.assert_called_once()
        mock_smtp_instance.quit.assert_called_once()

    @patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL")
    @patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP")
    def test_search_email_empty_results(self, mock_smtp, mock_imap):
        """Test search_email with no results"""
        mock_imap_instance = MagicMock()
        mock_imap.return_value = mock_imap_instance
        mock_imap_instance.login.return_value = ("OK", [])
        mock_imap_instance.select.return_value = ("OK", [])
        mock_imap_instance.uid.return_value = (None, [b""])

        connection_data = EmailConnectionDetails(
            email="test@example.com", password="password123"
        )
        client = EmailClient(connection_data)

        options = EmailSearchOptions(subject="NonexistentSubject")
        result = client.search_email(options)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestEmailHandlerUnit:
    """Unit tests for EmailHandler"""

    def test_init(self):
        """Test handler initialization"""
        handler = EmailHandler(
            name="test_handler",
            connection_data={"email": "test@example.com", "password": "password123"},
        )

        assert handler.name == "test_handler"
        assert handler.connection_data.email == "test@example.com"
        assert handler.is_connected is False
        assert handler.connection is None

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.EmailClient")
    def test_connect_success(self, mock_client):
        """Test successful connection"""
        handler = EmailHandler(
            name="test_handler",
            connection_data={"email": "test@example.com", "password": "password123"},
        )
        connection = handler.connect()
        assert handler.is_connected is True
        assert connection is not None
        mock_client.assert_called_once()

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.EmailClient")
    def test_connect_already_connected(self, mock_client):
        """Test connecting when already connected"""
        handler = EmailHandler(
            name="test_handler",
            connection_data={"email": "test@example.com", "password": "password123"},
        )
        handler.connect()
        first_connection = handler.connection
        second_connection = handler.connect()
        assert first_connection is second_connection
        assert mock_client.call_count == 1

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.EmailClient")
    def test_check_connection_success(self, mock_client):
        """Test check_connection with successful connection"""
        handler = EmailHandler(
            name="test_handler",
            connection_data={"email": "test@example.com", "password": "password123"},
        )

        response = handler.check_connection()

        assert response.success is True
        assert response.error_message is None

    @patch("mindsdb.integrations.handlers.email_handler.email_handler.EmailClient")
    def test_check_connection_failure(self, mock_client):
        """Test check_connection with connection failure"""
        mock_client.side_effect = Exception("Connection failed")

        handler = EmailHandler(
            name="test_handler",
            connection_data={"email": "test@example.com", "password": "wrong_password"},
        )

        response = handler.check_connection()

        assert response.success is False
        assert "Connection failed" in response.error_message
        assert handler.is_connected is False
