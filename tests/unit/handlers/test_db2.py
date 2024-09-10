from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock, Mock

from ibm_db_dbi import OperationalError

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.db2_handler.db2_handler import DB2Handler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class TestDB2Handler(BaseDatabaseHandlerTest, unittest.TestCase):

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

    # The tests for get_tables and get_columns methods do not follow the typical implementation pattern.
    # Therefore, the get_tables_query and get_columns_query properties are not used in the test cases.
    @property
    def get_tables_query(self):
        return ""

    @property
    def get_columns_query(self):
        return ""

    def test_get_tables(self):
        mock_client = Mock()
        mock_client.tables.return_value = [
            {
                'TABLE_NAME': 'table1',
                'TABLE_TYPE': 'TABLE',
                'TABLE_SCHEM': 'example_db'
            },
            {
                'TABLE_NAME': 'table2',
                'TABLE_TYPE': 'TABLE',
                'TABLE_SCHEM': 'example_db'
            }
        ]

        self.handler.connect = MagicMock(return_value=mock_client)

        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 2)
        self.assertEqual(df.columns.tolist(), ['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_TYPE'])
        self.assertEqual(df['TABLE_NAME'].tolist(), ['table1', 'table2'])

    def test_get_columns(self):
        mock_client = Mock()
        mock_client.columns.return_value = [
            {
                'COLUMN_NAME': 'col_1',
            },
            {
                'COLUMN_NAME': 'col_2',
            }
        ]

        self.handler.connect = MagicMock(return_value=mock_client)

        response = self.handler.get_columns(self.mock_table)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 2)
        self.assertEqual(df.columns.tolist(), ['COLUMN_NAME'])
        self.assertEqual(df['COLUMN_NAME'].tolist(), ['col_1', 'col_2'])

    def create_handler(self):
        return DB2Handler('db2', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('ibm_db_dbi.pconnect')


if __name__ == '__main__':
    unittest.main()
