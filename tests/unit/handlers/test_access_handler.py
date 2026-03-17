import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys

from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse, RESPONSE_TYPE

# Mock pyodbc and sqlalchemy_access before importing the handler
# This is necessary because the handler imports these at module level
if "pyodbc" not in sys.modules:
    sys.modules["pyodbc"] = MagicMock()
if "sqlalchemy_access" not in sys.modules:
    sys.modules["sqlalchemy_access"] = MagicMock()
    sys.modules["sqlalchemy_access.base"] = MagicMock()

from mindsdb.integrations.handlers.access_handler.access_handler import AccessHandler


class BaseAccessHandlerTest(unittest.TestCase):
    """Base test class with common setup and helper methods."""

    # Test constants
    TEST_DB_PATH = "C:\\Users\\test\\Documents\\test_db.accdb"
    TEST_HANDLER_NAME = "test_access_handler"

    def setUp(self):
        """Set up test fixtures."""
        self.connection_data = {"db_file": self.TEST_DB_PATH}
        self.handler = AccessHandler(self.TEST_HANDLER_NAME, self.connection_data)

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self.handler, "is_connected") and self.handler.is_connected:
            self.handler.disconnect()

    @staticmethod
    def create_mock_connection_with_cursor():
        """Create a mock connection with a cursor context manager."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        return mock_connection, mock_cursor

    def setup_mock_select_query(self, mock_cursor, columns, rows):
        """Configure mock cursor for SELECT query results."""
        mock_cursor.description = [(col,) for col in columns]
        mock_cursor.fetchall.return_value = rows

    def setup_mock_write_query(self, mock_cursor):
        """Configure mock cursor for INSERT/UPDATE/DELETE queries."""
        mock_cursor.fetchall.return_value = None

    def setup_mock_tables(self, mock_cursor, table_names):
        """Configure mock cursor for get_tables results."""
        mock_tables = []
        for name in table_names:
            mock_table = MagicMock()
            mock_table.table_name = name
            mock_tables.append(mock_table)
        mock_cursor.tables.return_value = mock_tables

    def setup_mock_columns(self, mock_cursor, columns_data):
        """Configure mock cursor for get_columns results.

        Args:
            mock_cursor: The mock cursor object
            columns_data: List of tuples [(column_name, type_name), ...]
        """
        mock_columns = []
        for col_name, type_name in columns_data:
            mock_col = MagicMock()
            mock_col.column_name = col_name
            mock_col.type_name = type_name
            mock_columns.append(mock_col)

        mock_cursor.columns.return_value = mock_columns


class TestAccessHandlerConnection(BaseAccessHandlerTest):
    """Test suite for Access Handler connection management."""

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_connect_success(self, mock_pyodbc, mock_platform):
        """Test successful connection to Access database."""
        mock_platform.return_value = "Windows"
        mock_connection, _ = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection

        result = self.handler.connect()

        mock_pyodbc.connect.assert_called_once_with(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + self.TEST_DB_PATH
        )

        self.assertTrue(self.handler.is_connected)
        self.assertEqual(result, mock_connection)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_connect_when_already_connected(self, mock_pyodbc, mock_platform):
        """Test that connect returns existing connection when already connected."""
        mock_platform.return_value = "Windows"
        mock_connection, _ = self.create_mock_connection_with_cursor()
        self.handler.connection = mock_connection
        self.handler.is_connected = True

        result = self.handler.connect()

        mock_pyodbc.connect.assert_not_called()
        self.assertEqual(result, mock_connection)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_connect_failure(self, mock_pyodbc, mock_platform):
        """Test connection failure handling."""
        mock_platform.return_value = "Windows"
        error_msg = "Driver not found"
        mock_pyodbc.connect.side_effect = Exception(error_msg)

        with self.assertRaises(Exception) as context:
            self.handler.connect()

        self.assertIn(error_msg, str(context.exception))
        self.assertFalse(self.handler.is_connected)

    def test_disconnect_when_connected(self):
        """Test disconnect when connection exists."""
        mock_connection, _ = self.create_mock_connection_with_cursor()
        self.handler.connection = mock_connection
        self.handler.is_connected = True

        result = self.handler.disconnect()

        mock_connection.close.assert_called_once()
        self.assertFalse(result)
        self.assertFalse(self.handler.is_connected)

    def test_disconnect_when_not_connected(self):
        """Test disconnect when not connected."""
        self.handler.is_connected = False

        result = self.handler.disconnect()

        self.assertIsNone(result)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_check_connection_success(self, mock_pyodbc, mock_platform):
        """Test successful connection check."""
        mock_platform.return_value = "Windows"
        mock_connection, _ = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection

        result = self.handler.check_connection()

        self.assertIsInstance(result, StatusResponse)
        self.assertTrue(result.success)
        self.assertFalse(self.handler.is_connected)  # Should disconnect after check

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_check_connection_failure(self, mock_pyodbc, mock_platform):
        """Test connection check failure."""
        mock_platform.return_value = "Windows"
        error_message = "Cannot open database"
        mock_pyodbc.connect.side_effect = Exception(error_message)

        result = self.handler.check_connection()

        self.assertIsInstance(result, StatusResponse)
        self.assertFalse(result.success)
        self.assertIn(error_message, result.error_message)


class TestAccessHandlerQueries(BaseAccessHandlerTest):
    """Test suite for Access Handler query execution."""

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_select_success(self, mock_pyodbc, mock_platform):
        """Test successful SELECT query execution."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection

        # Setup test data
        columns = ["id", "name", "email"]
        rows = [(1, "John Doe", "john@example.com"), (2, "Jane Smith", "jane@example.com")]
        self.setup_mock_select_query(mock_cursor, columns, rows)

        query = "SELECT * FROM customers"
        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        self.assertEqual(len(result.data_frame), 2)
        self.assertEqual(list(result.data_frame.columns), columns)
        mock_cursor.execute.assert_called_once_with(query)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_insert_success(self, mock_pyodbc, mock_platform):
        """Test successful INSERT query execution."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        self.setup_mock_write_query(mock_cursor)

        query = "INSERT INTO customers (name, email) VALUES ('Test User', 'test@example.com')"
        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.OK)
        mock_cursor.execute.assert_called_once_with(query)
        mock_connection.commit.assert_called_once()

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_update_success(self, mock_pyodbc, mock_platform):
        """Test successful UPDATE query execution."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        self.setup_mock_write_query(mock_cursor)

        query = "UPDATE customers SET status = 'active' WHERE id = 1"
        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.OK)
        mock_cursor.execute.assert_called_once_with(query)
        mock_connection.commit.assert_called_once()

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_delete_success(self, mock_pyodbc, mock_platform):
        """Test successful DELETE query execution."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        self.setup_mock_write_query(mock_cursor)

        query = "DELETE FROM customers WHERE id = 1"
        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.OK)
        mock_cursor.execute.assert_called_once_with(query)
        mock_connection.commit.assert_called_once()

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_empty_result(self, mock_pyodbc, mock_platform):
        """Test query with empty result set."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        mock_cursor.fetchall.return_value = []

        query = "SELECT * FROM customers WHERE id = 999"
        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.OK)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_failure(self, mock_pyodbc, mock_platform):
        """Test query execution failure."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection

        error_message = "Syntax error in query"
        mock_cursor.execute.side_effect = Exception(error_message)

        query = "SELECT * FROM invalid_table"
        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
        self.assertIn(error_message, result.error_message)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_disconnects_when_needed(self, mock_pyodbc, mock_platform):
        """Test that native_query disconnects when it opened the connection."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        self.setup_mock_write_query(mock_cursor)

        self.handler.is_connected = False
        self.handler.native_query("SELECT 1")

        self.assertFalse(self.handler.is_connected)

    @patch("mindsdb.integrations.handlers.access_handler.access_handler.AccessDialect", MagicMock())
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.SqlalchemyRender")
    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_query_with_ast(self, mock_pyodbc, mock_platform, mock_render):
        """Test query method with AST input."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        self.setup_mock_write_query(mock_cursor)

        mock_renderer = MagicMock()
        mock_render.return_value = mock_renderer
        mock_renderer.get_string.return_value = "SELECT * FROM test_table"

        from mindsdb_sql_parser import parse_sql

        ast_query = parse_sql("SELECT * FROM test_table")

        result = self.handler.query(ast_query)

        self.assertEqual(result.type, RESPONSE_TYPE.OK)
        mock_renderer.get_string.assert_called_once_with(ast_query, with_failback=True)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_native_query_with_special_characters(self, mock_pyodbc, mock_platform):
        """Test query with special characters."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        self.setup_mock_select_query(mock_cursor, ["name"], [("O'Brien",)])

        query = "SELECT * FROM customers WHERE name = 'O''Brien'"
        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        mock_cursor.execute.assert_called_once_with(query)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_multiple_sequential_queries(self, mock_pyodbc, mock_platform):
        """Test multiple sequential queries."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        self.setup_mock_write_query(mock_cursor)

        queries = ["INSERT INTO test VALUES (1)", "INSERT INTO test VALUES (2)", "INSERT INTO test VALUES (3)"]
        results = [self.handler.native_query(q) for q in queries]

        for result in results:
            self.assertEqual(result.type, RESPONSE_TYPE.OK)


class TestAccessHandlerMetadata(BaseAccessHandlerTest):
    """Test suite for Access Handler metadata operations."""

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_get_tables_success(self, mock_pyodbc, mock_platform):
        """Test successful retrieval of table list."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection

        table_names = ["customers", "orders"]
        self.setup_mock_tables(mock_cursor, table_names)

        result = self.handler.get_tables()

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        self.assertEqual(len(result.data_frame), 2)
        for name in table_names:
            self.assertIn(name, result.data_frame["table_name"].values)
        mock_cursor.tables.assert_called_once_with(tableType="Table")

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_get_tables_empty(self, mock_pyodbc, mock_platform):
        """Test get_tables with no tables."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        mock_cursor.tables.return_value = []

        result = self.handler.get_tables()

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        self.assertEqual(len(result.data_frame), 0)

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_get_columns_success(self, mock_pyodbc, mock_platform):
        """Test successful retrieval of column list."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection

        columns_data = [("id", "INTEGER"), ("name", "VARCHAR"), ("email", "VARCHAR")]
        self.setup_mock_columns(mock_cursor, columns_data)

        result = self.handler.get_columns("customers")

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        self.assertEqual(len(result.data_frame), 3)
        self.assertEqual(list(result.data_frame.columns), ["column_name", "data_type"])
        for col_name, _ in columns_data:
            self.assertIn(col_name, result.data_frame["column_name"].values)
        mock_cursor.columns.assert_called_once_with(table="customers")

    @patch("platform.system")
    @patch("mindsdb.integrations.handlers.access_handler.access_handler.pyodbc")
    def test_get_columns_empty(self, mock_pyodbc, mock_platform):
        """Test get_columns with no columns."""
        mock_platform.return_value = "Windows"
        mock_connection, mock_cursor = self.create_mock_connection_with_cursor()
        mock_pyodbc.connect.return_value = mock_connection
        mock_cursor.columns.return_value = []

        result = self.handler.get_columns("empty_table")

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        self.assertEqual(len(result.data_frame), 0)


class TestAccessHandlerConnectionArgs(BaseAccessHandlerTest):
    """Test suite for Access Handler connection arguments validation."""

    def test_connection_args_structure(self):
        """Test that connection_args has the correct structure."""
        from mindsdb.integrations.handlers.access_handler.connection_args import connection_args

        required_fields = ["type", "description", "required", "label"]
        self.assertIn("db_file", connection_args)
        for field in required_fields:
            self.assertIn(field, connection_args["db_file"])
        self.assertTrue(connection_args["db_file"]["required"])

    def test_connection_args_example_structure(self):
        """Test that connection_args_example has the correct structure."""
        from mindsdb.integrations.handlers.access_handler.connection_args import connection_args_example

        self.assertIn("db_file", connection_args_example)
        self.assertIsInstance(connection_args_example["db_file"], str)

    def test_handler_initialization(self):
        """Test handler initialization with valid connection data."""
        handler = AccessHandler("test_handler", self.connection_data)

        self.assertEqual(handler.name, "test_handler")
        self.assertEqual(handler.connection_data, self.connection_data)
        self.assertEqual(handler.dialect, "access")
        self.assertFalse(handler.is_connected)
        self.assertIsNone(handler.connection)


class TestAccessHandlerEdgeCases(BaseAccessHandlerTest):
    """Test suite for Access Handler edge cases."""

    def test_handler_del_when_connected(self):
        """Test __del__ method when handler is connected."""
        handler = AccessHandler("test_handler", self.connection_data)
        mock_connection, _ = self.create_mock_connection_with_cursor()
        handler.connection = mock_connection
        handler.is_connected = True

        handler.__del__()

        mock_connection.close.assert_called_once()

    def test_handler_del_when_not_connected(self):
        """Test __del__ method when handler is not connected."""
        handler = AccessHandler("test_handler", self.connection_data)
        handler.is_connected = False

        try:
            handler.__del__()
        except Exception as e:
            self.fail(f"__del__ raised exception: {e}")


if __name__ == "__main__":
    unittest.main()
