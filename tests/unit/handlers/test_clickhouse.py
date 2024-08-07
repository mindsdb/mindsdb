from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from sqlalchemy.exc import SQLAlchemyError

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.clickhouse_handler.clickhouse_handler import ClickHouseHandler


class TestClickHouseHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            port=8123,
            user='example_user',
            password='example_pass',
            database='example_db',
            protocol='native'
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return SQLAlchemyError("Connection Failed")

    @property
    def get_tables_query(self):
        return f"SHOW TABLES FROM {self.dummy_connection_data['database']}"

    @property
    def get_columns_query(self):
        return f"DESCRIBE {self.mock_table}"

    def create_handler(self):
        return ClickHouseHandler('clickhouse', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.clickhouse_handler.clickhouse_handler.create_engine', return_value=MagicMock())

    def test_initialization(self):
        """Test if the handler initializes with correct values and defaults."""
        self.mock_connect.return_value = MagicMock()
        self.assertEqual(self.handler.name, "clickhouse")
        self.assertEqual(self.handler.dialect, "clickhouse")
        self.assertFalse(self.handler.is_connected)
        self.assertEqual(self.handler.protocol, "native")

    def test_connect_success(self):
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()
        self.mock_connect.assert_called_once_with(
            f"clickhouse+{self.dummy_connection_data['protocol']}://{self.dummy_connection_data['user']}:{self.dummy_connection_data['password']}@{self.dummy_connection_data['host']}:{self.dummy_connection_data['port']}/{self.dummy_connection_data['database']}"
        )


if __name__ == '__main__':
    unittest.main()
