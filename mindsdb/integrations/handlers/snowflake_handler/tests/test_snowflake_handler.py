import unittest
from unittest.mock import Mock, patch
import os
import pandas as pd

from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class TestSnowflakeHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for all tests."""
        cls.test_connection_data = {
            "host": os.environ.get("MDB_TEST_SNOWFLAKE_HOST", "test.snowflakecomputing.com"),
            "account": os.environ.get("MDB_TEST_SNOWFLAKE_ACCOUNT", "test_account"),
            "user": os.environ.get("MDB_TEST_SNOWFLAKE_USER", "test_user"),
            "password": os.environ.get("MDB_TEST_SNOWFLAKE_PASSWORD", "test_password"),
            "database": os.environ.get("MDB_TEST_SNOWFLAKE_DATABASE", "test_db"),
            "schema": os.environ.get("MDB_TEST_SNOWFLAKE_SCHEMA", "public"),
            "warehouse": os.environ.get("MDB_TEST_SNOWFLAKE_WAREHOUSE", "test_wh"),
            "role": os.environ.get("MDB_TEST_SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        }
        cls.handler_kwargs = {"connection_data": cls.test_connection_data}

    def setUp(self):
        """Set up for each test method."""
        # Mock the Snowflake connector
        self.mock_connection = Mock()
        self.mock_cursor = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor

        # Mock cursor responses
        self.mock_cursor.fetchall.return_value = [
            ("test_table", "BASE TABLE", "public"),
            ("another_table", "BASE TABLE", "public"),
        ]
        self.mock_cursor.description = [
            Mock(name="table_name", type_code="VARCHAR"),
            Mock(name="table_type", type_code="VARCHAR"),
            Mock(name="table_schema", type_code="VARCHAR"),
        ]

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_01_connect_success(self, mock_snowflake_connector):
        """Test successful connection to Snowflake."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        self.assertTrue(handler.is_connected)
        mock_snowflake_connector.connect.assert_called_once()

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_02_connect_failure(self, mock_snowflake_connector):
        """Test connection failure handling."""
        mock_snowflake_connector.connect.side_effect = Exception("Connection failed")

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)

        # Connection should fail but not crash
        self.assertFalse(handler.is_connected)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_03_check_connection(self, mock_snowflake_connector):
        """Test connection status checking."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        response = handler.check_connection()
        self.assertTrue(response.success)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_04_check_connection_failure(self, mock_snowflake_connector):
        """Test connection status check when connection fails."""
        mock_snowflake_connector.connect.side_effect = Exception("Connection failed")

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)

        response = handler.check_connection()
        self.assertFalse(response.success)
        self.assertIsNotNone(response.error_message)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_05_native_query_select(self, mock_snowflake_connector):
        """Test native query execution for SELECT statements."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock query result
        self.mock_cursor.fetchall.return_value = [("John", 25, "Engineer"), ("Jane", 30, "Manager")]
        self.mock_cursor.description = [
            Mock(name="name", type_code="VARCHAR"),
            Mock(name="age", type_code="NUMBER"),
            Mock(name="role", type_code="VARCHAR"),
        ]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        query = "SELECT name, age, role FROM employees LIMIT 2"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)
        self.mock_cursor.execute.assert_called_with(query)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_06_native_query_insert(self, mock_snowflake_connector):
        """Test native query execution for INSERT statements."""
        mock_snowflake_connector.connect.return_value = self.mock_connection
        self.mock_cursor.rowcount = 1

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        query = "INSERT INTO employees (name, age) VALUES ('Bob', 28)"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.OK)
        self.mock_cursor.execute.assert_called_with(query)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_07_native_query_error(self, mock_snowflake_connector):
        """Test native query error handling."""
        mock_snowflake_connector.connect.return_value = self.mock_connection
        self.mock_cursor.execute.side_effect = Exception("SQL execution error")

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        query = "SELECT * FROM non_existent_table"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIsNotNone(response.error_message)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_08_get_tables(self, mock_snowflake_connector):
        """Test getting list of tables."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock INFORMATION_SCHEMA query result
        self.mock_cursor.fetchall.return_value = [
            ("TEST_DB", "PUBLIC", "EMPLOYEES", "BASE TABLE"),
            ("TEST_DB", "PUBLIC", "DEPARTMENTS", "BASE TABLE"),
        ]
        self.mock_cursor.description = [
            Mock(name="TABLE_CATALOG", type_code="VARCHAR"),
            Mock(name="TABLE_SCHEMA", type_code="VARCHAR"),
            Mock(name="TABLE_NAME", type_code="VARCHAR"),
            Mock(name="TABLE_TYPE", type_code="VARCHAR"),
        ]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        response = handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)
        self.assertIn("TABLE_NAME", response.data_frame.columns)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_09_get_columns(self, mock_snowflake_connector):
        """Test getting columns for a specific table."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock INFORMATION_SCHEMA.COLUMNS query result
        self.mock_cursor.fetchall.return_value = [
            ("TEST_DB", "PUBLIC", "EMPLOYEES", "ID", 1, None, "YES", "NUMBER", None, None, 38, 0),
            ("TEST_DB", "PUBLIC", "EMPLOYEES", "NAME", 2, None, "NO", "VARCHAR", 255, None, None, None),
            ("TEST_DB", "PUBLIC", "EMPLOYEES", "AGE", 3, None, "YES", "NUMBER", None, None, 38, 0),
        ]
        self.mock_cursor.description = [
            Mock(name="TABLE_CATALOG", type_code="VARCHAR"),
            Mock(name="TABLE_SCHEMA", type_code="VARCHAR"),
            Mock(name="TABLE_NAME", type_code="VARCHAR"),
            Mock(name="COLUMN_NAME", type_code="VARCHAR"),
            Mock(name="ORDINAL_POSITION", type_code="NUMBER"),
            Mock(name="COLUMN_DEFAULT", type_code="VARCHAR"),
            Mock(name="IS_NULLABLE", type_code="VARCHAR"),
            Mock(name="DATA_TYPE", type_code="VARCHAR"),
            Mock(name="CHARACTER_MAXIMUM_LENGTH", type_code="NUMBER"),
            Mock(name="CHARACTER_OCTET_LENGTH", type_code="NUMBER"),
            Mock(name="NUMERIC_PRECISION", type_code="NUMBER"),
            Mock(name="NUMERIC_SCALE", type_code="NUMBER"),
        ]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        response = handler.get_columns("employees")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 3)
        self.assertIn("COLUMN_NAME", response.data_frame.columns)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_10_query_with_ast(self, mock_snowflake_connector):
        """Test query execution with AST nodes."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock query result
        self.mock_cursor.fetchall.return_value = [("test_value",)]
        self.mock_cursor.description = [Mock(name="column1", type_code="VARCHAR")]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        # Create a simple AST node (this would normally come from SQL parser)
        from mindsdb_sql_parser.ast import Select, Identifier

        ast_query = Select(targets=[Identifier("column1")], from_table=Identifier("test_table"))

        with patch.object(handler, "renderer") as mock_renderer:
            mock_renderer.get_string.return_value = "SELECT column1 FROM test_table"
            response = handler.query(ast_query)

            self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
            mock_renderer.get_string.assert_called_once()

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_11_disconnect(self, mock_snowflake_connector):
        """Test disconnection from Snowflake."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()
        self.assertTrue(handler.is_connected)

        handler.disconnect()
        self.assertFalse(handler.is_connected)
        self.mock_connection.close.assert_called_once()

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_12_meta_get_primary_keys(self, mock_snowflake_connector):
        """Test getting primary key information."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock primary key query result
        self.mock_cursor.fetchall.return_value = [("PRIMARY", "EMPLOYEES", "ID")]
        self.mock_cursor.description = [
            Mock(name="CONSTRAINT_TYPE", type_code="VARCHAR"),
            Mock(name="TABLE_NAME", type_code="VARCHAR"),
            Mock(name="COLUMN_NAME", type_code="VARCHAR"),
        ]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        response = handler.meta_get_primary_keys("employees")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        if len(response.data_frame) > 0:
            self.assertIn("COLUMN_NAME", response.data_frame.columns)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_13_meta_get_foreign_keys(self, mock_snowflake_connector):
        """Test getting foreign key information."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock foreign key query result (empty for this test)
        self.mock_cursor.fetchall.return_value = []
        self.mock_cursor.description = [
            Mock(name="CONSTRAINT_NAME", type_code="VARCHAR"),
            Mock(name="TABLE_NAME", type_code="VARCHAR"),
            Mock(name="COLUMN_NAME", type_code="VARCHAR"),
            Mock(name="REFERENCED_TABLE_NAME", type_code="VARCHAR"),
            Mock(name="REFERENCED_COLUMN_NAME", type_code="VARCHAR"),
        ]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        response = handler.meta_get_foreign_keys("employees")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    def test_14_connection_args_validation(self):
        """Test connection arguments validation."""
        from mindsdb.integrations.handlers.snowflake_handler.connection_args import connection_args

        # Test required fields
        required_fields = ["host", "user", "password", "database"]
        for field in required_fields:
            self.assertIn(field, connection_args)
            self.assertTrue(connection_args[field]["required"])

        # Test optional fields
        optional_fields = ["account", "schema", "warehouse", "role"]
        for field in optional_fields:
            self.assertIn(field, connection_args)
            self.assertFalse(connection_args[field].get("required", False))

    def test_15_type_mapping(self):
        """Test Snowflake to MySQL type mapping."""
        from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import _map_type
        from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE

        # Test common type mappings
        self.assertEqual(_map_type("NUMBER"), MYSQL_DATA_TYPE.DECIMAL)
        self.assertEqual(_map_type("VARCHAR"), MYSQL_DATA_TYPE.VARCHAR)
        self.assertEqual(_map_type("TIMESTAMP"), MYSQL_DATA_TYPE.TIMESTAMP)
        self.assertEqual(_map_type("BOOLEAN"), MYSQL_DATA_TYPE.TINYINT)

        # Test unknown type defaults to VAR_STRING
        self.assertEqual(_map_type("UNKNOWN_TYPE"), MYSQL_DATA_TYPE.VAR_STRING)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_16_large_result_handling(self, mock_snowflake_connector):
        """Test handling of large result sets."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock a large result set
        large_result = [("row_" + str(i), i) for i in range(10000)]
        self.mock_cursor.fetchall.return_value = large_result
        self.mock_cursor.description = [Mock(name="name", type_code="VARCHAR"), Mock(name="id", type_code="NUMBER")]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        query = "SELECT name, id FROM large_table"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(response.data_frame), 10000)

    @patch("mindsdb.integrations.handlers.snowflake_handler.snowflake_handler.connector")
    def test_17_special_characters_handling(self, mock_snowflake_connector):
        """Test handling of special characters in queries and results."""
        mock_snowflake_connector.connect.return_value = self.mock_connection

        # Mock result with special characters
        self.mock_cursor.fetchall.return_value = [
            ("O'Connor", "Data with 'quotes'"),
            ("Smith & Jones", "Unicode: café naïve résumé"),
        ]
        self.mock_cursor.description = [
            Mock(name="name", type_code="VARCHAR"),
            Mock(name="description", type_code="VARCHAR"),
        ]

        handler = SnowflakeHandler("test_snowflake", **self.handler_kwargs)
        handler.connect()

        query = "SELECT name, description FROM test_table WHERE name LIKE '%''%'"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(response.data_frame), 2)

    def tearDown(self):
        """Clean up after each test."""
        # Reset mocks for the next test
        if hasattr(self, "mock_cursor"):
            self.mock_cursor.reset_mock()
        if hasattr(self, "mock_connection"):
            self.mock_connection.reset_mock()


if __name__ == "__main__":
    unittest.main()
