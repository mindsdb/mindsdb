from collections import OrderedDict
import unittest
from unittest.mock import patch

from teradatasql import OperationalError

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.teradata_handler.teradata_handler import TeradataHandler


class TestTeradataHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            user='example_user',
            password='example_pass',
            database='example_db',
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return OperationalError("Connection Failed")

    @property
    def get_tables_query(self):
        return f"""
            SELECT
                TableName AS table_name,
                TableKind AS table_type
            FROM DBC.TablesV
            WHERE DatabaseName = '{self.dummy_connection_data['database']}'
            AND (TableKind = 'T'
                OR TableKind = 'O'
                OR TableKind = 'Q'
                OR TableKind = 'V')
        """

    @property
    def get_columns_query(self):
        return f"""
            SELECT ColumnName AS "Field",
                   ColumnType AS "Type"
            FROM DBC.ColumnsV
            WHERE DatabaseName = '{self.dummy_connection_data['database']}'
            AND TableName = '{self.mock_table}'
        """

    def create_handler(self):
        return TeradataHandler('mysql', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('teradatasql.connect')


if __name__ == '__main__':
    unittest.main()
