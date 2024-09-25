from collections import OrderedDict
import unittest
from unittest.mock import patch

from pyhive.exc import OperationalError

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.hive_handler.hive_handler import HiveHandler


class TestHiveHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            username='example_user',
            password='example_pass',
            database='example_db',
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return OperationalError("Connection Failed")

    @property
    def get_tables_query(self):
        return "SHOW TABLES"

    @property
    def get_columns_query(self):
        return f"DESCRIBE {self.mock_table}"

    def create_handler(self):
        return HiveHandler('hive', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.hive_handler.hive_handler.hive.Connection')


if __name__ == '__main__':
    unittest.main()
