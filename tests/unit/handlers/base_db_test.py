from abc import ABC, abstractmethod
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


class BaseDBTest(ABC):
    mock_table = 'mock_table'

    def setUp(self):
        """
        Set up the test environment by creating instances of the patcher and handler.
        This method should be called by the `setUp` method of the subclass.
        The `setUp` method of subclasses should also set the following attributes:
        - `dummy_connection_data`: A dictionary containing dummy connection data.
        - `err_to_raise_on_connect_failure`: An exception to raise when the connection fails.
        - `get_tables_query`: A SQL query to get the list of tables.
        - `get_columns_query`: A SQL query to get the columns of a table.
        """
        self.patcher = self.create_patcher()
        self.mock_connect = self.patcher.start()
        self.handler = self.create_handler()

    def tearDown(self):
        self.patcher.stop()

    @abstractmethod
    def create_patcher(self):
        """
        Create and return a unittest.mock.patch instance for the package used to implement the connection.
        This method should be overridden in subclasses to provide the specific patch instance.
        """
        pass

    @abstractmethod
    def create_handler(self):
        """
        Create and return a handler instance.
        This method should be overridden in subclasses to provide the specific handler.
        """
        pass

    def test_connect_success(self):
        """
        Tests if the `connect` method handles a successful connection and sets `is_connected` to True.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Tests if the `connect` method handles a failed connection and sets `is_connected` to False.
        """
        self.mock_connect.side_effect = self.err_to_raise_on_connect_failure

        with self.assertRaises(type(self.err_to_raise_on_connect_failure)):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection_success(self):
        """
        Tests if the `check_connection` method handles a successful connection check and returns a StatusResponse object that accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertTrue(response.success)
        self.assertFalse(response.error_message)

    def test_check_connection_failure(self):
        """
        Tests if the `check_connection` method handles a failed connection check and returns a StatusResponse object that accurately reflects the connection status.
        """
        self.mock_connect.side_effect = self.err_to_raise_on_connect_failure
        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertFalse(response.success)
        self.assertTrue(response.error_message)

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor and returns a Response object.
        """
        mock_conn = MagicMock()
        mock_cursor = CursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        query_str = f"SELECT * FROM {self.mock_table}"
        data = self.handler.native_query(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)

    def test_get_columns(self):
        """
        Tests if the `get_tables` method calls `native_query` with the correct SQL query.
        """
        self.handler.native_query = MagicMock()
        self.handler.get_columns(self.mock_table)

        self.handler.native_query.assert_called_once_with(self.get_columns_query)

    def test_get_tables(self):
        """
        Tests if the `get_columns` method constructs the correct SQL query and if it calls `native_query` with that query.
        """
        self.handler.native_query = MagicMock()
        self.handler.get_tables()

        self.handler.native_query.assert_called_once_with(self.get_tables_query)
