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
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_ID AS ORDINAL_POSITION,
                DATA_DEFAULT AS COLUMN_DEFAULT,
                CASE NULLABLE WHEN 'Y' THEN 'YES' ELSE 'NO' END AS IS_NULLABLE,
                CHAR_LENGTH AS CHARACTER_MAXIMUM_LENGTH,
                NULL AS CHARACTER_OCTET_LENGTH,
                DATA_PRECISION AS NUMERIC_PRECISION,
                DATA_SCALE AS NUMERIC_SCALE,
                NULL AS DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                NULL AS COLLATION_NAME
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{self.mock_table}'
            ORDER BY TABLE_NAME, COLUMN_ID;
        """

    def create_handler(self):
        return OracleHandler('oracle', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.oracle_handler.oracle_handler.connect')


if __name__ == '__main__':
    unittest.main()
