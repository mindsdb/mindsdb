import unittest
from unittest.mock import patch, MagicMock, Mock
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
        self.patcher = patch('sqlalchemy.create_engine', return_value=MagicMock())
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
