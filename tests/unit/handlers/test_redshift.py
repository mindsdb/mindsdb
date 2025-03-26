import unittest
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import psycopg

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers.redshift_handler.redshift_handler import RedshiftHandler
from test_postgres import TestPostgresHandler


class TestRedshiftHandler(TestPostgresHandler):

    def create_handler(self):
        return RedshiftHandler('redshift', connection_data=self.dummy_connection_data)

    def test_insert(self):
        """
        Tests the `insert` method to ensure it correctly inserts a DataFrame into a table and returns the appropriate response.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

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
        mock_conn.commit.assert_called_once()

    def test_insert_error(self):
        """
        Tests the `insert` method to ensure it correctly handles an exception and returns the appropriate response.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        error_msg = "Table doesn't exist"
        error = psycopg.Error(error_msg)
        mock_cursor.executemany.side_effect = error

        df = pd.DataFrame({
            'column1': [1, 2, 3, np.nan],
            'column2': ['a', 'b', 'c', None]
        })

        response = self.handler.insert('nonexistent_table', df)

        mock_cursor.executemany.assert_called_once()
        mock_conn.rollback.assert_called_once()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertEqual(response.error_message, error_msg)

    def test_insert_with_empty_dataframe(self):
        """
        Tests the `insert` method with an empty DataFrame to ensure it handles this edge case correctly.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        df = pd.DataFrame(columns=['column1', 'column2'])

        table_name = 'mock_table'
        response = self.handler.insert(table_name, df)

        columns = ', '.join([f'"{col}"' if ' ' in col else col for col in df.columns])
        values = ', '.join(['%s' for _ in range(len(df.columns))])
        expected_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'

        mock_cursor.executemany.assert_called_once()
        call_args, call_kwargs = mock_cursor.executemany.call_args
        self.assertEqual(call_args[0], expected_query)
        self.assertEqual(len(call_args[1]), 0)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.OK)

        mock_conn.commit.assert_called_once()

    def test_insert_with_special_column_names(self):
        """
        Tests the `insert` method with column names that contain spaces and special characters
        to verify proper quoting of column names.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        df = pd.DataFrame({
            'normal_column': [1, 2],
            'column with spaces': ['a', 'b'],
            'column-with-hyphens': [True, False],
            'mixed@column#123': [3.14, 2.71]
        })

        table_name = 'mock_table'
        response = self.handler.insert(table_name, df)

        call_args = mock_cursor.executemany.call_args[0][0]

        for col in df.columns:
            if ' ' in col:
                self.assertIn(f'"{col}"', call_args)
            else:
                self.assertTrue(col in call_args or f'"{col}"' in call_args)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.OK)

    def test_insert_disconnect_when_needed(self):
        """
        Tests that the `insert` method disconnects when it created the connection
        but keeps the connection open if it was already connected.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.is_connected = False
        self.handler.connect = MagicMock(return_value=mock_conn)
        self.handler.disconnect = MagicMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        df = pd.DataFrame({'column1': [1, 2, 3]})
        self.handler.insert('mock_table', df)
        self.handler.disconnect.assert_called_once()
        self.handler.connect.reset_mock()
        self.handler.disconnect.reset_mock()
        self.handler.is_connected = True
        self.handler.insert('mock_table', df)
        self.handler.disconnect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
