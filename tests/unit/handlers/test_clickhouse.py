import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
from collections import OrderedDict
from mindsdb.integrations.handlers.clickhouse_handler.clickhouse_handler import ClickHouseHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response
)


class TestClickHouseHandler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        host='127.0.0.1',
        port=8123,
        user='example_user',
        password='example_pass',
        database='example_db',
        protocol='native'
    )

    def setUp(self):
        self.patcher = patch('mindsdb.integrations.handlers.clickhouse_handler.clickhouse_handler.create_engine', return_value=MagicMock())
        self.mock_connect = self.patcher.start()
        self.mock_engine = MagicMock()
        self.handler = ClickHouseHandler("test_handler", connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_initialization(self):
        """Test if the handler initializes with correct values and defaults."""
        self.mock_connect.return_value = MagicMock()
        self.assertEqual(self.handler.name, "test_handler")
        self.assertEqual(self.handler.dialect, "clickhouse")
        self.assertFalse(self.handler.is_connected)
        self.assertEqual(self.handler.protocol, "native")

    def test_connect(self):
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()
        self.mock_connect.assert_called_once_with(
            f"clickhouse+{self.dummy_connection_data['protocol']}://{self.dummy_connection_data['user']}:{self.dummy_connection_data['password']}@{self.dummy_connection_data['host']}:{self.dummy_connection_data['port']}/{self.dummy_connection_data['database']}"
        )

    def test_check_connection(self):
        self.mock_connect.return_value = MagicMock()
        connected = self.handler.check_connection()
        self.assertTrue(connected)

    def test_connect_failure(self):
        self.mock_connect.side_effect = SQLAlchemyError("Connection Failed")

        with self.assertRaises(SQLAlchemyError):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_native_query(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertTrue(mock_cursor.execute.called)

    def test_get_columns(self):
        self.handler.native_query = MagicMock()

        table_name = 'mock_table'
        self.handler.get_columns(table_name)

        expected_query = f"DESCRIBE {table_name}"
        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_tables(self):
        self.handler.native_query = MagicMock()
        self.handler.get_tables()

        expected_query = f"SHOW TABLES FROM {self.dummy_connection_data['database']}"
        self.handler.native_query.assert_called_once_with(expected_query)


if __name__ == '__main__':
    unittest.main()
