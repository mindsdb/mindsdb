import unittest
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from base_handler_test import MockCursorContextManager
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers.redshift_handler.redshift_handler import RedshiftHandler
from test_postgres import TestPostgresHandler


class TestRedshiftHandler(TestPostgresHandler):

    def create_handler(self):
        return RedshiftHandler('redshift', connection_data={'connection_data': self.dummy_connection_data})

    def test_insert(self):
        """
        Tests the `insert` method to ensure it correctly inserts a DataFrame into a table and returns the appropriate response.
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.executemany.return_value = None

        df = pd.DataFrame({
            'column1': [1, 2, 3, np.nan],
            'column2': ['a', 'b', 'c', None]
        })

        table_name = 'mock_table'
        response = self.handler.insert(table_name, df)

        columns = ', '.join([f'"{col}"' if ' ' in col else col for col in df.columns])
        values = ', '.join(['%s' for _ in range(len(df.columns))])
        expected_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'

        mock_cursor.executemany.assert_called_once_with(expected_query, df.replace({np.nan: None}).values.tolist())
        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.OK)


if __name__ == '__main__':
    test_classes_to_run = [TestRedshiftHandler]

    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    big_suite = unittest.TestSuite(suites_list)

    runner = unittest.TextTestRunner()
    results = runner.run(big_suite)
