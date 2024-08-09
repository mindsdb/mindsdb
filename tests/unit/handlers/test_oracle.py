from collections import OrderedDict
import unittest
from unittest.mock import patch

from oracledb import DatabaseError

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.oracle_handler.oracle_handler import OracleHandler


class TestOracleHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            user='example_user',
            password='example_pass',
            dsn='example_dsn'
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return DatabaseError("Connection Failed")

    @property
    def get_tables_query(self):
        return """
            SELECT table_name
            FROM user_tables
            ORDER BY 1
        """

    @property
    def get_columns_query(self):
        return f"""
            SELECT
                column_name,
                data_type
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{self.mock_table}'
        """

    def create_handler(self):
        return OracleHandler('oracle', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.oracle_handler.oracle_handler.connect')


if __name__ == '__main__':
    unittest.main()
