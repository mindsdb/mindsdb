from collections import OrderedDict
import unittest
from unittest.mock import patch

import snowflake

from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler
from tests.unit.handlers.base_handler_test import BaseDatabaseHandlerTest


class TestSnowflakeHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    def setUp(self):
        self.dummy_connection_data = OrderedDict(
            account='tvuibdy-vm85921',
            user='example_user',
            password='example_pass',
            database='example_db',
        )

        self.err_to_raise_on_connect_failure = snowflake.connector.errors.Error("Connection Failed")

        self.get_tables_query = """
            SELECT TABLE_NAME, TABLE_SCHEMA, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
              AND TABLE_SCHEMA = current_schema()
        """

        self.get_columns_query = f"""
            SELECT COLUMN_NAME AS FIELD, DATA_TYPE AS TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{self.mock_table}'
              AND TABLE_SCHEMA = current_schema()
        """

        return super().setUp()

    def create_handler(self):
        return SnowflakeHandler('snowflake', connection_data=self.dummy_connection_data)
    
    def create_patcher(self):
        return patch('snowflake.connector.connect')


if __name__ == '__main__':
    unittest.main()
