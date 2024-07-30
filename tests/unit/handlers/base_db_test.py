from unittest.mock import MagicMock, Mock

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    description = [['a']]

    def fetchall(self):
        return [[1]]


class BaseDBTest():

    def setUp(self):
        self.patcher = self.create_patcher()
        self.mock_connect = self.patcher.start()
        self.handler = self.create_handler()

    def tearDown(self):
        self.patcher.stop()

    def create_patcher(self):
        """
        Create a patcher object for the package used to implement the connection.
        This method should be overridden in subclasses to provide the specific handler.
        """
        raise NotImplementedError("Subclasses should implement this method to return a patcher object.")

    def create_handler(self):
        """
        Create and return a handler instance.
        This method should be overridden in subclasses to provide the specific handler.
        """
        raise NotImplementedError("Subclasses should implement this method to return a handler instance.")

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

    def test_check_connection_success(self):
        """
        Tests if the `check_connection` method handles a successful connection check and returns a StatusResponse object that accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        response = self.handler.check_connection()
        self.assertTrue(response)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor and returns a Response object.
        """
        mock_conn = MagicMock()
        mock_cursor = CursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)

    def test_get_columns(self):
        """
        Tests if the `get_tables` method to confirm it correctly calls `native_query` with the appropriate SQL commands.
        """
        self.handler.native_query = MagicMock()

        table_name = 'mock_table'
        self.handler.get_columns(table_name)

        self.handler.native_query.assert_called_once_with(self.get_columns_query)

    def test_get_tables(self):
        """
        Tests if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()
        self.handler.get_tables()

        self.handler.native_query.assert_called_once_with(self.get_tables_query)

