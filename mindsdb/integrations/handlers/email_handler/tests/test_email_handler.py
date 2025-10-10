from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from mindsdb.integrations.handlers.email_handler.email_handler import EmailHandler
from mindsdb.integrations.handlers.email_handler.email_client import EmailClient
from mindsdb.integrations.handlers.email_handler.settings import EmailSearchOptions


class TestEmailHandlerAdvanced:
    def test_advanced_settings_precedence_passthrough(self):
        # Given advanced settings, they should take precedence over defaults.
        handler = EmailHandler(
            connection_data={
                "email": "user@proton.me",
                "password": "secret",
                "imap_host": "127.0.0.1",
                "imap_port": 1143,
                "imap_use_ssl": False,
                "imap_use_starttls": True,
                "imap_username": "user@localhost",
                "smtp_host": "127.0.0.1",
                "smtp_port": 1025,
                "smtp_starttls": False,
            }
        )

        with (
            patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4") as mock_imap4,
            patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP") as mock_smtp,
        ):
            mock_imap4.return_value = MagicMock()
            mock_smtp.return_value = MagicMock()

            client = handler.connect()
            assert isinstance(client, EmailClient)

            # Ensure IMAP4 (non-SSL) was used with host/port from advanced settings
            mock_imap4.assert_called_once_with("127.0.0.1", 1143)

            # Ensure SMTP used host/port from advanced settings
            mock_smtp.assert_called_once_with("127.0.0.1", 1025)

            # Ensure username override is set
            assert client.imap_username == "user@localhost"

            handler.disconnect()

    def test_check_connection_success_and_failure(self):
        # Success path
        handler_ok = EmailHandler(connection_data={"email": "e", "password": "p"})
        with (
            patch.object(EmailClient, "select_mailbox", return_value=None),
            patch.object(EmailClient, "logout", return_value=None),
            patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL") as mock_ssl,
            patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP") as mock_smtp,
        ):
            mock_ssl.return_value = MagicMock()
            mock_smtp.return_value = MagicMock()

            resp_ok = handler_ok.check_connection()
            assert resp_ok.success is True
            assert not resp_ok.error_message

        # Failure path: simulate constructor raising
        with (
            patch(
                "mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL",
                side_effect=RuntimeError("boom"),
            ),
            patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP") as mock_smtp,
        ):
            mock_smtp.return_value = MagicMock()
            handler_fail = EmailHandler(connection_data={"email": "e", "password": "p"})
            resp_fail = handler_fail.check_connection()
            assert resp_fail.success is False
            assert "Error connecting to Email" in (resp_fail.error_message or "")

    def test_mailbox_sanitization(self):
        handler = EmailHandler(connection_data={"email": "e", "password": "p"})
        with (
            patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL") as mock_ssl,
            patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP") as mock_smtp,
        ):
            imap = MagicMock()
            mock_ssl.return_value = imap
            mock_smtp.return_value = MagicMock()

            client = handler.connect()
            # login/select should fail on traversal mailbox
            imap.login.return_value = ("OK", [])
            imap.select.return_value = ("OK", [])
            with pytest.raises(ValueError):
                client.select_mailbox("INBOX/../../etc")
            handler.disconnect()

    def test_uid_fetch_uses_comma_separated_and_chunking(self):
        handler = EmailHandler(connection_data={"email": "e", "password": "p"})
        with (
            patch("mindsdb.integrations.handlers.email_handler.email_client.imaplib.IMAP4_SSL") as mock_ssl,
            patch("mindsdb.integrations.handlers.email_handler.email_client.smtplib.SMTP") as mock_smtp,
            patch.object(EmailClient, "_UID_FETCH_CHUNK", 2),
        ):  # Force multiple chunks for this test
            imap = MagicMock()
            mock_ssl.return_value = imap
            mock_smtp.return_value = MagicMock()

            # Arrange IMAP responses
            imap.login.return_value = ("OK", [])
            imap.select.return_value = ("OK", [])
            # Simulate 3 UIDs
            imap.uid.side_effect = [
                # First call: search
                ("OK", [b"1 2 3"]),
                # Second call: fetch chunk 1 (1,2)
                ("OK", [(b"1 (UID 1 RFC822 {10})", b"From: a\r\nSubject: s\r\n\r\nbody")]),
                # Third call: fetch chunk 2 (3)
                ("OK", [(b"3 (UID 3 RFC822 {10})", b"From: a\r\nSubject: s\r\n\r\nbody")]),
            ]

            client = handler.connect()
            df = client.search_email(EmailSearchOptions(mailbox="INBOX"))
            assert isinstance(df, pd.DataFrame)

            # Assert uid was called for search and then for fetch twice
            assert imap.uid.call_count == 3
            # Validate the comma-separated list used in fetch
            # 2nd call args
            fetch_args_1 = imap.uid.call_args_list[1].args
            assert fetch_args_1[0] == "fetch"
            assert fetch_args_1[1] == "1,2"
            # 3rd call args
            fetch_args_2 = imap.uid.call_args_list[2].args
            assert fetch_args_2[0] == "fetch"
            assert fetch_args_2[1] == "3"

            handler.disconnect()
