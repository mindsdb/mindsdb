import unittest
from unittest.mock import Mock, patch
import os
import pandas as pd

from mindsdb.integrations.handlers.mariadb_handler.mariadb_handler import MariaDBHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class TestMariaDBHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for all tests."""
        cls.test_connection_data = {
            "host": os.environ.get("MDB_TEST_MARIADB_HOST", "localhost"),
            "port": int(os.environ.get("MDB_TEST_MARIADB_PORT", "3306")),
            "user": os.environ.get("MDB_TEST_MARIADB_USER", "root"),
            "password": os.environ.get("MDB_TEST_MARIADB_PASSWORD", "test_password"),
            "database": os.environ.get("MDB_TEST_MARIADB_DATABASE", "test_db"),
            "ssl": os.environ.get("MDB_TEST_MARIADB_SSL", "false").lower() == "true",
        }
        cls.handler_kwargs = {"connection_data": cls.test_connection_data}

    def setUp(self):
        """Set up for each test method."""
        # Mock the mysql.connector
        self.mock_connection = Mock()
        self.mock_cursor = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor

        # Mock cursor responses
        self.mock_cursor.fetchall.return_value = [("employees", "BASE TABLE"), ("departments", "BASE TABLE")]
        self.mock_cursor.description = [Mock(name="TABLE_NAME"), Mock(name="TABLE_TYPE")]

    @patch("mysql.connector.connect")
    def test_01_connect_success(self, mock_mysql_connect):
        """Test successful connection to MariaDB."""
        mock_mysql_connect.return_value = self.mock_connection

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        self.assertTrue(handler.is_connected)
        mock_mysql_connect.assert_called_once()

    @patch("mysql.connector.connect")
    def test_02_connect_failure(self, mock_mysql_connect):
        """Test connection failure handling."""
        mock_mysql_connect.side_effect = Exception("Connection failed")

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)

        # Connection should fail but not crash
        self.assertFalse(handler.is_connected)

    @patch("mysql.connector.connect")
    def test_03_check_connection(self, mock_mysql_connect):
        """Test connection status checking."""
        mock_mysql_connect.return_value = self.mock_connection

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        response = handler.check_connection()
        self.assertTrue(response.success)

    @patch("mysql.connector.connect")
    def test_04_native_query_select(self, mock_mysql_connect):
        """Test native query execution for SELECT statements."""
        mock_mysql_connect.return_value = self.mock_connection

        # Mock query result
        self.mock_cursor.fetchall.return_value = [("John", 25, "Engineering"), ("Jane", 30, "Marketing")]
        self.mock_cursor.description = [Mock(name="name"), Mock(name="age"), Mock(name="department")]

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        query = "SELECT name, age, department FROM employees LIMIT 2"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)

    @patch("mysql.connector.connect")
    def test_05_native_query_insert(self, mock_mysql_connect):
        """Test native query execution for INSERT statements."""
        mock_mysql_connect.return_value = self.mock_connection
        self.mock_cursor.rowcount = 1

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        query = "INSERT INTO employees (name, age, department) VALUES ('Bob', 28, 'Sales')"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.OK)

    @patch("mysql.connector.connect")
    def test_06_get_tables(self, mock_mysql_connect):
        """Test getting list of tables."""
        mock_mysql_connect.return_value = self.mock_connection

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        response = handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("mysql.connector.connect")
    def test_07_get_columns(self, mock_mysql_connect):
        """Test getting columns for a specific table."""
        mock_mysql_connect.return_value = self.mock_connection

        # Mock DESCRIBE query result
        self.mock_cursor.fetchall.return_value = [
            ("id", "int(11)", "NO", "PRI", None, "auto_increment"),
            ("name", "varchar(100)", "NO", "", None, ""),
            ("age", "int(11)", "YES", "", None, ""),
        ]
        self.mock_cursor.description = [
            Mock(name="Field"),
            Mock(name="Type"),
            Mock(name="Null"),
            Mock(name="Key"),
            Mock(name="Default"),
            Mock(name="Extra"),
        ]

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        response = handler.get_columns("employees")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("mysql.connector.connect")
    def test_08_mariadb_specific_features(self, mock_mysql_connect):
        """Test MariaDB-specific features like JSON columns and sequences."""
        mock_mysql_connect.return_value = self.mock_connection

        # Mock JSON query result
        self.mock_cursor.fetchall.return_value = [
            ('{"name": "John", "skills": ["Python", "SQL"]}',),
            ('{"name": "Jane", "skills": ["JavaScript", "React"]}',),
        ]
        self.mock_cursor.description = [Mock(name="profile_json")]

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        # Test JSON column query
        query = "SELECT profile_json FROM user_profiles WHERE JSON_EXTRACT(profile_json, '$.name') = 'John'"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("mysql.connector.connect")
    def test_09_transaction_support(self, mock_mysql_connect):
        """Test transaction handling in MariaDB."""
        mock_mysql_connect.return_value = self.mock_connection

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        # Test transaction queries
        begin_response = handler.native_query("BEGIN")
        self.assertEqual(begin_response.type, RESPONSE_TYPE.OK)

        insert_response = handler.native_query("INSERT INTO test_table (id, name) VALUES (1, 'test')")
        self.assertEqual(insert_response.type, RESPONSE_TYPE.OK)

        commit_response = handler.native_query("COMMIT")
        self.assertEqual(commit_response.type, RESPONSE_TYPE.OK)

    @patch("mysql.connector.connect")
    def test_10_error_handling(self, mock_mysql_connect):
        """Test error handling for invalid queries."""
        mock_mysql_connect.return_value = self.mock_connection
        self.mock_cursor.execute.side_effect = Exception("Table doesn't exist")

        handler = MariaDBHandler("test_mariadb", **self.handler_kwargs)
        handler.connect()

        query = "SELECT * FROM non_existent_table"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIsNotNone(response.error_message)

    def test_11_connection_args_validation(self):
        """Test connection arguments validation."""
        from mindsdb.integrations.handlers.mariadb_handler.connection_args import connection_args

        # Test required fields
        required_fields = ["host", "user", "password"]
        for field in required_fields:
            self.assertIn(field, connection_args)
            self.assertTrue(connection_args[field]["required"])

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, "mock_cursor"):
            self.mock_cursor.reset_mock()
        if hasattr(self, "mock_connection"):
            self.mock_connection.reset_mock()


if __name__ == "__main__":
    unittest.main()
