import unittest
import snowflake
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestSnowflakeHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        account='tvuibdy-vm85921',
        user='example_user',
        password='example_pass',
        database='example_db',
    )

    def setUp(self):
        self.patcher = patch('snowflake.connector.connect')
        self.mock_connect = self.patcher.start()
        self.handler = SnowflakeHandler('snowflake', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that snowflake.connector.connect is called exactly once.
        """

        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Ensures that the connect method correctly handles a connection failure.
        by raising a snowflake.connector.errors.Error and sets is_connected to False.
        """

        self.mock_connect.side_effect = snowflake.connector.errors.Error("Connection Failed")

        with self.assertRaises(snowflake.connector.errors.Error):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection(self):
        """
        Verifies that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """

        self.mock_connect.return_value = MagicMock()
        connected = self.handler.check_connection()
        self.assertTrue(connected)
        assert isinstance(connected, StatusResponse)
        self.assertFalse(connected.error_message)

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor and returns a Response object.
        """

        mock_conn = MagicMock()
        mock_cursor = CursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = None

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)
        mock_cursor.execute.assert_called_once_with(query_str)
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)

    def test_get_columns(self):
        """
        Checks if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """

        self.handler.native_query = MagicMock()

        table_name = 'mock_table'
        self.handler.get_columns(table_name)

        expected_query = f"""SHOW COLUMNS IN TABLE {table_name};"""

        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_tables(self):
        """
        Tests the `get_tables` method to confirm it correctly calls `native_query` with the appropriate SQL commands.
        """

        self.handler.native_query = MagicMock()
        self.handler.get_tables()

        expected_query_tables = "SHOW TABLES;"
        expected_query_views = "SHOW VIEWS;"

        assert self.handler.native_query.call_count == 2
        self.handler.native_query.assert_any_call(expected_query_tables)
        self.handler.native_query.assert_any_call(expected_query_views)


if __name__ == '__main__':
    unittest.main()