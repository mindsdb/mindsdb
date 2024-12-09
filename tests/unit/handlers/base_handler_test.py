from abc import ABC, abstractmethod
from unittest.mock import MagicMock, Mock

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)


class MockCursorContextManager(Mock):
    """
    A mock class that simulates a cursor context manager for database clients.
    This class is used in the `BaseDatabaseHandlerTest` class to simulate the cursor object returned by the database client.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    description = [['a']]

    def fetchall(self):
        return [[1]]


class BaseHandlerTestSetup(ABC):
    """
    The base class that provides setup and teardown methods for tests.
    It is recommended to use a 'base' subclass of `BaseHandlerTest` as a base class for testing handlers.
    This class can be used as a base class to only set up the test environment for testing handlers.
    """

    @property
    @abstractmethod
    def dummy_connection_data(self):
        """
        A dictionary containing dummy connection data.
        This attribute should be overridden in subclasses to provide the specific connection data.
        """
        pass

    def setUp(self):
        """
        Sets up the test environment by creating instances of the patcher and handler.
        """
        self.patcher = self.create_patcher()
        self.mock_connect = self.patcher.start()
        self.handler = self.create_handler()

    def tearDown(self):
        """
        Tears down the test environment by stopping the patcher.
        """
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


class BaseHandlerTest(BaseHandlerTestSetup):
    """
    The Base class for testing handlers. This class provides methods to test the `connect` and `check_connection` methods of a handler.
    It is recommended to use a 'base' subclass of this class as a base class for testing handlers.
    This class can be used as a base class for testing only the connection-related methods of a handler.
    """

    @property
    @abstractmethod
    def err_to_raise_on_connect_failure(self):
        """
        An exception to raise when the connection fails. This is the exception that is raised in the `connect` and `check_connection` methods when the connection fails.
        This attribute should be overridden in subclasses to provide the specific exception.
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


class BaseDatabaseHandlerTest(BaseHandlerTest):
    """
    The Base class for testing database handlers. This class provides methods to test the `native_query`, `get_tables` and `get_columns` methods of a handler.
    This class should be used as a base class for testing database handlers which have an implementation that uses a typical database client (with a cursor) like psycopg2, pymssql, etc.
    """

    @property
    def mock_table(self):
        """
        A string containing the name of a mock table. This attribute should be used as the table name when constructing SQL queries.
        It should be used in the `get_columns_query` attribute.
        """
        return 'mock_table'

    @property
    @abstractmethod
    def get_tables_query(self):
        """
        A string containing the SQL query to get the tables of a database. This is the query that is executed in the `get_tables` method.
        This attribute should be overridden in subclasses to provide the specific query.
        """
        pass

    @property
    @abstractmethod
    def get_columns_query(self):
        """
        A string containing the SQL query to get the columns of a table. This is the query that is executed in the `get_columns` method.
        This attribute should be overridden in subclasses to provide the specific query.
        """
        pass

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor and returns a Response object.
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

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


class BaseAPIHandlerTest(BaseHandlerTest):
    """
    The Base class for testing API handlers. This class provides methods to test the `get_tables` and `get_columns` methods of a handler.
    The 'native_query' should be tested in the individual subclasses because not all API handlers support it.
    """

    @property
    @abstractmethod
    def registered_tables(self):
        """
        A list of tables that are registered to the handler.
        This attribute should be overridden in subclasses to provide the specific list of tables.
        """
        pass

    def test_get_columns(self):
        """
        Tests if the `get_columns` method returns the list of columns of a table.
        """
        response = self.handler.get_columns(self.registered_tables[0])

        assert isinstance(response, Response)
        assert response.data_frame.columns.tolist() == ['Field', 'Type']

    def test_get_tables(self):
        """
        Tests if the `get_tables` method returns the list of registered tables.
        """
        response = self.handler.get_tables()

        assert isinstance(response, Response)
        assert all(col in response.data_frame.columns.tolist() for col in ['table_name', 'table_type'])
        assert response.data_frame['table_type'].unique().tolist() == ['BASE TABLE']
        assert response.data_frame['table_name'].tolist() == self.registered_tables


class BaseAPIChatHandlerTest(BaseAPIHandlerTest):
    """
    The Base class for testing chat handlers. This class provides methods to test `get_chat_config` and `get_my_user_name` methods of a handler.
    If a `subscribe` method is implemented, it should be tested in the individual subclass.
    """

    def test_get_chat_config(self):
        """
        Tests if the `get_chat_config` method returns a chat configuration in the form of a dictionary.
        The dictionary should contain the keys `polling` and optionally, either `chat_table` or `tables`.
        If either `chat_table` or `tables` is present, they should contain the keys `name`, `chat_id_col`, `username_col`, `text_col`, and `time_col`.
        """
        response = self.handler.get_chat_config()

        assert isinstance(response, dict)
        assert 'polling' in response and isinstance(response['polling'], dict) and 'type' in response['polling'] and response['polling']['type'] in ['realtime', 'message_count', 'webhook']

        required_keys = ['name', 'chat_id_col', 'username_col', 'text_col', 'time_col']
        if 'chat_table' in response:
            assert isinstance(response['chat_table'], dict)
            assert all(key in list(response['chat_table'].keys()) for key in required_keys)

        if 'tables' in response:
            assert isinstance(response['tables'], list)
            assert all(isinstance(table, dict) for table in response['tables'])
            assert all(all(key in list(table['chat_table'].keys()) for key in required_keys) for table in response['tables'])

    @abstractmethod
    def test_get_my_user_name(self):
        """
        Tests if the `get_my_user_name` method returns the name of the user.
        This should be overridden in subclasses to provide the specific test.
        """
        pass


class BaseAPIResourceTestSetup(BaseHandlerTestSetup):
    """
    The base class that provides setup and teardown methods for testing implementations of the `APIResource` class.
    """

    def setUp(self):
        """
        Sets up the test environment by creating an instance of the resource along with the patcher and handler from the parent class.
        """
        super().setUp()
        self.resource = self.create_resource()

    @abstractmethod
    def create_resource(self):
        """
        Create and return an instance of the resource.
        This method should be overridden in subclasses to provide the specific resource.
        """
        pass
