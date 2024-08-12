from collections import OrderedDict
import unittest
from unittest.mock import patch

import pymssql

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler


class TestMSSQLHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            port=1433,
            user='example_user',
            password='example_pass',
            database='example_db',
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return pymssql.OperationalError("Connection Failed")

    @property
    def get_tables_query(self):
        return f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM {self.dummy_connection_data['database']}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE in ('BASE TABLE', 'VIEW');
        """

    @property
    def get_columns_query(self):
        return f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{self.mock_table}'
        """

    def create_handler(self):
        return SqlServerHandler('mssql', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('pymssql.connect')


if __name__ == '__main__':
    unittest.main()
