from collections import OrderedDict
import unittest
from unittest.mock import patch

import snowflake

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler


class TestSnowflakeHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            account='tvuibdy-vm85921',
            user='example_user',
            password='example_pass',
            database='example_db',
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return snowflake.connector.errors.Error("Connection Failed")

    @property
    def get_tables_query(self):
        return """
            SELECT TABLE_NAME, TABLE_SCHEMA, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
              AND TABLE_SCHEMA = current_schema()
        """

    @property
    def get_columns_query(self):
        return f"""
            SELECT COLUMN_NAME AS FIELD, DATA_TYPE AS TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{self.mock_table}'
              AND TABLE_SCHEMA = current_schema()
        """

    def create_handler(self):
        return SnowflakeHandler('snowflake', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('snowflake.connector.connect')


if __name__ == '__main__':
    unittest.main()
