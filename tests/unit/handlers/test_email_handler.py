import datetime as dt
import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz

from mindsdb_sql_parser import parse_sql

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.handlers.email_handler.email_handler import EmailHandler
from mindsdb.integrations.handlers.email_handler.email_tables import EmailsTable
from mindsdb.integrations.handlers.email_handler.email_ingestor import EmailIngestor
from mindsdb.integrations.handlers.email_handler.settings import EmailSearchOptions


class TestEmailHandler(BaseHandlerTestSetup, unittest.TestCase):
    """Test suite for Email Handler"""

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            email="test@example.com",
            password="test_password",
            imap_server="imap.gmail.com",
            smtp_server="smtp.gmail.com",
            smtp_port=587,
        )

    def create_handler(self):
        return EmailHandler("email", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("imaplib.IMAP4_SSL")

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)

    def test_connect_already_connected(self):
        """Test that connect returns existing connection when already connected."""
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()

        # Mark as connected and try to connect again
        self.handler.is_connected = True
        connection = self.handler.connect()

        self.assertIs(connection, self.handler.connection)

    def test_check_connection(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        response = self.handler.check_connection()

        self.assertTrue(response.success)
        self.assertFalse(response.error_message)

    def test_select(self):
        """Test the select method of EmailsTable class."""
        # Setup mock connection
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()

        emails_table = EmailsTable(self.handler)

        # Create mock data
        mock_df = pd.DataFrame(
            {
                "date": [
                    "Wed, 02 Feb 2022 15:30:00 +0000",
                    "Thu, 10 Mar 2022 10:45:15 +0530",
                    "Fri, 16 Dec 2022 20:15:30 -0400",
                ],
                "body_content_type": ["html", "html", "text"],
                "body": [
                    "<html><body><p>Hello, World!</p></body></html>",
                    "<html><body><p>Hello, World!</p></body></html>",
                    "Hello, World!",
                ],
                "from_field": [
                    "sender1@example.com",
                    "sender2@example.com",
                    "sender3@example.com",
                ],
                "id": ["1", "2", "3"],
                "to_field": [
                    "recipient@example.com",
                    "recipient@example.com",
                    "recipient@example.com",
                ],
                "subject": ["Test 1", "Test 2", "Test 3"],
            }
        )

        self.handler.connection.search_email = MagicMock(return_value=mock_df)

        query = parse_sql("SELECT * FROM emails limit 1")
        result = emails_table.select(query)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(self.handler.connection.search_email.called)

    def test_select_invalid_column(self):
        """Test that select with invalid column raises Exception."""
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()

        emails_table = EmailsTable(self.handler)
        query = parse_sql("SELECT invalid_column FROM emails limit 1")

        with self.assertRaises(Exception):
            emails_table.select(query)

    def test_insert(self):
        """Test the insert method of EmailsTable class."""
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()

        emails_table = EmailsTable(self.handler)
        self.handler.connection.send_email = MagicMock()

        query = parse_sql(
            "INSERT INTO email_datasource.emails(to_field, subject, body) "
            'VALUES ("toemail@email.com", "MindsDB", "Hello from MindsDB!")'
        )

        emails_table.insert(query)
        self.assertTrue(self.handler.connection.send_email.called)

    def test_insert_invalid_column(self):
        """Test that insert with invalid column raises Exception."""
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()

        emails_table = EmailsTable(self.handler)
        query = parse_sql(
            "INSERT INTO email_datasource.emails(to_field, subject, body, invalid_column) "
            'VALUES ("toemail@email.com", "MindsDB", "blaha" , "invalid")'
        )

        with self.assertRaises(Exception):
            emails_table.insert(query)

    def test_get_columns(self):
        """Test the get_columns method of EmailsTable class."""
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()

        emails_table = EmailsTable(self.handler)
        columns = emails_table.get_columns()

        self.assertIsInstance(columns, list)
        self.assertIn("id", columns)
        self.assertIn("body", columns)
        self.assertIn("subject", columns)
        self.assertIn("to_field", columns)
        self.assertIn("from_field", columns)
        self.assertIn("datetime", columns)

    # ========== Bug fix tests ==========

    @patch("mindsdb.integrations.handlers.email_handler.email_ingestor.chardet.detect")
    def test_encoding_none_handling(self, mock_chardet):
        """
        Test that EmailIngestor handles None encoding gracefully.
        This tests the fix for the bug where chardet.detect returns None.
        """
        # Mock chardet to return None encoding
        mock_chardet.return_value = {"encoding": None}

        # Create mock email client
        mock_client = MagicMock()
        mock_client.search_email.return_value = pd.DataFrame(
            {
                "id": ["1"],
                "body": [b"Test email body"],
                "subject": ["Test"],
                "to_field": ["test@example.com"],
                "from_field": ["sender@example.com"],
                "date": ["Wed, 02 Feb 2022 15:30:00 +0000"],
                "body_content_type": ["plain"],
            }
        )

        # Create EmailIngestor and test it doesn't crash
        search_options = EmailSearchOptions()
        ingestor = EmailIngestor(mock_client, search_options)
        result_df = ingestor.ingest()

        # Verify the result is a DataFrame and has data
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertGreater(len(result_df), 0)
        # Verify chardet was called
        self.assertTrue(mock_chardet.called)

    @patch("mindsdb.integrations.handlers.email_handler.email_ingestor.chardet.detect")
    def test_encoding_windows_handling(self, mock_chardet):
        """
        Test that EmailIngestor handles Windows encoding correctly.
        """
        # Mock chardet to return Windows encoding
        mock_chardet.return_value = {"encoding": "windows-1252"}

        # Create mock email client
        mock_client = MagicMock()
        mock_client.search_email.return_value = pd.DataFrame(
            {
                "id": ["1"],
                "body": [b"Test email body"],
                "subject": ["Test"],
                "to_field": ["test@example.com"],
                "from_field": ["sender@example.com"],
                "date": ["Wed, 02 Feb 2022 15:30:00 +0000"],
                "body_content_type": ["plain"],
            }
        )

        # Create EmailIngestor and test it doesn't crash
        search_options = EmailSearchOptions()
        ingestor = EmailIngestor(mock_client, search_options)
        result_df = ingestor.ingest()

        # Verify the result is a DataFrame and has data
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertGreater(len(result_df), 0)

    def test_parse_date_naive_datetime(self):
        """
        Test that parse_date correctly handles naive datetimes.
        This tests the fix for the timezone handling bug.
        """
        # Test with date string (naive datetime)
        date_str = "2022-02-02 15:30:00"
        result = EmailsTable.parse_date(date_str)

        # Verify result is timezone-aware and in UTC
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.tzinfo.zone, "UTC")

    def test_parse_date_with_timezone(self):
        """
        Test that parse_date correctly handles timezone-aware datetimes.
        """
        aware_dt = dt.datetime(
            2022, 2, 2, 15, 30, 0, tzinfo=pytz.timezone("US/Eastern")
        )
        result = EmailsTable.parse_date(aware_dt)

        # Should return the same datetime
        self.assertEqual(result, aware_dt)

    def test_parse_date_string_formats(self):
        """
        Test that parse_date handles different date string formats.
        """
        # Test date without time
        date_str = "2022-02-02"
        result = EmailsTable.parse_date(date_str)
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.year, 2022)
        self.assertEqual(result.month, 2)
        self.assertEqual(result.day, 2)


if __name__ == "__main__":
    unittest.main()
