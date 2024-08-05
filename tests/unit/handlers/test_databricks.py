import unittest
from databricks.sql import RequestError
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.handlers.databricks_handler.databricks_handler import DatabricksHandler


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestDatabricksHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        server_hostname='adb-1234567890123456.7.azuredatabricks.net',
        http_path='sql/protocolv1/o/1234567890123456/1234-567890-test123',
        access_token='dapi1234567890ab1cde2f3ab456c7d89efa'
    )

    def setUp(self):
        self.patcher = patch('databricks.sql.client.Connection')
        self.mock_connect = self.patcher.start()
        self.handler = DatabricksHandler('databricks', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        """
        Tests if the `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that databricks.sql.client.Connection is called exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Tests if the `connect` method correctly handles a connection failure by raising a databricks.sql.RequestError and sets is_connected to False.
        There can be other exceptions that can be raised by the `connect` method such as RuntimeError etc.
        """
        self.mock_connect.side_effect = RequestError("Connection Failed")

        with self.assertRaises(RequestError):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection_success(self):
        """
        Tests if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        response = self.handler.check_connection()
        self.assertTrue(response)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_check_connection_failure(self):
        """
        Tests if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        self.mock_connect.side_effect = RequestError("Connection Failed")

        response = self.handler.check_connection()
        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

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

    def test_get_tables(self):
        """
        Tests if the `get_tables` method to confirm it correctly calls `native_query` with the appropriate SQL commands.
        """
        self.handler.native_query = MagicMock()

        self.handler.get_tables()

        expected_query = """
            SHOW TABLES;
        """

        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_columns(self):
        """
        Tests if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()

        table_name = 'mock_table'
        self.handler.get_columns(table_name)

        expected_query = f"""DESCRIBE TABLE {table_name};"""

        self.handler.native_query.assert_called_once_with(expected_query)


if __name__ == '__main__':
    unittest.main()
