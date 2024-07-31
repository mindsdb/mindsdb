from collections import OrderedDict
import pymssql
import unittest
from unittest.mock import patch

from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler
from tests.unit.handlers.base_db_test import BaseDBTest


class TestMSSQLHandler(BaseDBTest, unittest.TestCase):

    def setUp(self):
        self.dummy_connection_data = OrderedDict(
            host='127.0.0.1',
            port=1433,
            user='example_user',
            password='example_pass',
            database='example_db',
        )

        self.err_to_raise_on_connect_failure = pymssql.OperationalError("Connection Failed")

        self.get_tables_query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM {self.dummy_connection_data['database']}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE in ('BASE TABLE', 'VIEW');
        """

        self.get_columns_query = f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{self.mock_table}'
        """

        return super().setUp()

    def create_handler(self):
        return SqlServerHandler('mssql', connection_data=self.dummy_connection_data)
    
    def create_patcher(self):
        return patch('pymssql.connect')


if __name__ == '__main__':
    unittest.main()
