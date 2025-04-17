from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

import snowflake
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    INF_SCHEMA_COLUMNS_NAMES_SET,
    RESPONSE_TYPE
)


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
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                COLUMN_DEFAULT,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                COLLATION_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{self.mock_table}'
              AND TABLE_SCHEMA = current_schema()
        """

    def create_handler(self):
        return SnowflakeHandler('snowflake', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('snowflake.connector.connect')

    def test_connect_validation(self):
        """
        Tests that connect method raises ValueError when required connection parameters are missing
        """
        # Test missing 'account'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['account']
        handler = SnowflakeHandler('snowflake', connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'user'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['user']
        handler = SnowflakeHandler('snowflake', connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'password'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['password']
        handler = SnowflakeHandler('snowflake', connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'database'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['database']
        handler = SnowflakeHandler('snowflake', connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

    def test_disconnect(self):
        """
        Tests the disconnect method to ensure it correctly closes connections
        """
        mock_conn = MagicMock()
        self.handler.connection = mock_conn
        self.handler.is_connected = True
        self.handler.disconnect()

        mock_conn.close.assert_called_once()
        self.assertFalse(self.handler.is_connected)
        self.handler.is_connected = False
        mock_conn.reset_mock()
        self.handler.disconnect()
        mock_conn.close.assert_not_called()

    def test_check_connection(self):
        """
        Tests the check_connection method to ensure it properly tests connectivity
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)

        response = self.handler.check_connection()
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with('select 1;')
        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)

        self.handler.connect = MagicMock()
        connect_error = snowflake.connector.errors.Error("Connection error")
        self.handler.connect.side_effect = connect_error

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertEqual(response.error_message, str(connect_error))

    def test_native_query_with_results(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and handles the case
        where the query returns a result set (e.g., SELECT).
        It uses fetch_pandas_batches() for Snowflake.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock(spec=snowflake.connector.cursor.DictCursor)

        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor

        expected_columns = ['ID', 'NAME']
        batch1_data = [(1, 'test1')]
        batch2_data = [(2, 'test2')]
        mock_df_batch1 = DataFrame(batch1_data, columns=expected_columns)
        mock_df_batch2 = DataFrame(batch2_data, columns=expected_columns)
        mock_cursor.fetch_pandas_batches.return_value = iter([mock_df_batch1, mock_df_batch2])

        mock_cursor.description = [('ID',), ('NAME',)]
        mock_cursor.rowcount = 2

        query_str = "SELECT ID, NAME FROM test_table"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(snowflake.connector.DictCursor)
        mock_cursor.execute.assert_called_once_with(query_str)
        mock_cursor.fetch_pandas_batches.assert_called_once()
        mock_cursor.fetchall.assert_not_called()

        self.assertIsInstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(data.data_frame, DataFrame)
        self.assertListEqual(list(data.data_frame.columns), expected_columns)
        self.assertEqual(len(data.data_frame), 2)
        self.assertEqual(data.data_frame.iloc[0]['ID'], 1)
        self.assertEqual(data.data_frame.iloc[0]['NAME'], 'test1')
        self.assertEqual(data.data_frame.iloc[1]['ID'], 2)
        self.assertEqual(data.data_frame.iloc[1]['NAME'], 'test2')

        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_not_called()

    def test_native_query_no_results(self):
        """
        Tests the `native_query` method to ensure it executes a non-SELECT SQL query (e.g., INSERT)
        and correctly returns RESPONSE_TYPE.OK by simulating the NotSupportedError fallback.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock(spec=snowflake.connector.cursor.DictCursor)

        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = None
        mock_cursor.fetch_pandas_batches.side_effect = snowflake.connector.errors.NotSupportedError()
        mock_cursor.fetchall.return_value = [{'number of rows inserted': 1}]
        mock_cursor.rowcount = 1

        query_str = "INSERT INTO test_table VALUES (1, 'test')"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(snowflake.connector.DictCursor)
        mock_cursor.execute.assert_called_once_with(query_str)
        mock_cursor.fetch_pandas_batches.assert_called_once()

        self.assertIsInstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.OK)
        self.assertEqual(data.affected_rows, 1)

        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_not_called()

    def test_native_query_error(self):
        """
        Tests the `native_query` method to ensure it properly handles and returns database errors
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        error_msg = "Syntax error in SQL statement"
        error = snowflake.connector.errors.ProgrammingError(msg=error_msg)
        mock_cursor.execute.side_effect = error

        query_str = "INVALID SQL"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query_str)

        self.assertIsInstance(data, Response)
        self.assertEqual(data.type, RESPONSE_TYPE.ERROR)
        self.assertIn(error_msg, data.error_message)

        mock_conn.rollback.assert_not_called()
        mock_conn.commit.assert_not_called()

    def test_query_method(self):
        """
        Tests the query method to ensure it correctly converts ASTNode to SQL and calls native_query
        """
        orig_renderer = getattr(self.handler, 'renderer', None)
        renderer_mock = MagicMock()
        renderer_mock.get_string.return_value = "SELECT * FROM test_table_rendered"

        self.handler.native_query = MagicMock()
        expected_response = Response(RESPONSE_TYPE.TABLE)
        self.handler.native_query.return_value = expected_response

        try:
            if orig_renderer:
                self.handler.renderer = renderer_mock

            mock_ast = MagicMock()
            result = self.handler.query(mock_ast)

            if orig_renderer:
                renderer_mock.get_string.assert_called_once_with(mock_ast, with_failback=True)
                expected_query = "SELECT * FROM test_table_rendered"
            else:
                expected_query = str(mock_ast)

            self.handler.native_query.assert_called_once_with(expected_query)
            self.assertEqual(result, expected_response)

        finally:
            if orig_renderer:
                self.handler.renderer = orig_renderer
            del self.handler.native_query

    def test_get_tables(self):
        """
        Tests that get_tables calls native_query with the correct SQL for Snowflake
        """
        expected_response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=DataFrame([('table1', 'SCHEMA1', 'BASE TABLE')], columns=['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_TYPE'])
        )
        self.handler.native_query = MagicMock(return_value=expected_response)

        response = self.handler.get_tables()

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]

        self.assertIn('FROM INFORMATION_SCHEMA.TABLES', call_args)
        self.assertIn('TABLE_NAME', call_args)
        self.assertIn('TABLE_SCHEMA', call_args)
        self.assertIn('TABLE_TYPE', call_args)
        self.assertIn('current_schema()', call_args)
        self.assertIn("('BASE TABLE', 'VIEW')", call_args)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)
        self.assertListEqual(list(response.data_frame.columns), ['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_TYPE'])

        del self.handler.native_query

    def test_get_columns(self):
        """
        Tests that get_columns calls native_query with the correct SQL for Snowflake
        and returns the expected DataFrame structure.
        """
        query_columns = [
            'COLUMN_NAME', 'DATA_TYPE', 'ORDINAL_POSITION', 'COLUMN_DEFAULT', 'IS_NULLABLE',
            'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION',
            'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME'
        ]

        expected_df_data = [
            {
                'COLUMN_NAME': 'COL1', 'DATA_TYPE': 'VARCHAR', 'ORDINAL_POSITION': 1, 'COLUMN_DEFAULT': None,
                'IS_NULLABLE': 'YES', 'CHARACTER_MAXIMUM_LENGTH': 255, 'CHARACTER_OCTET_LENGTH': None,
                'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None, 'DATETIME_PRECISION': None,
                'CHARACTER_SET_NAME': None, 'COLLATION_NAME': None
            },
            {
                'COLUMN_NAME': 'COL2', 'DATA_TYPE': 'NUMBER', 'ORDINAL_POSITION': 2, 'COLUMN_DEFAULT': '0',
                'IS_NULLABLE': 'NO', 'CHARACTER_MAXIMUM_LENGTH': None, 'CHARACTER_OCTET_LENGTH': None,
                'NUMERIC_PRECISION': 38, 'NUMERIC_SCALE': 0, 'DATETIME_PRECISION': None,
                'CHARACTER_SET_NAME': None, 'COLLATION_NAME': None
            }
        ]
        expected_df = DataFrame(expected_df_data, columns=query_columns)

        expected_response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=expected_df
        )
        self.handler.native_query = MagicMock(return_value=expected_response)

        table_name = "test_table"
        response = self.handler.get_columns(table_name)

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]
        self.assertIn('FROM INFORMATION_SCHEMA.COLUMNS', call_args)
        self.assertIn(f"TABLE_NAME = '{table_name}'", call_args)
        self.assertIn('COLUMN_NAME', call_args)
        self.assertIn('DATA_TYPE', call_args)
        self.assertIn('IS_NULLABLE', call_args)
        self.assertNotIn('MYSQL_DATA_TYPE', call_args)

        self.assertEqual(response.type, RESPONSE_TYPE.COLUMNS_TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)

        #  Verify  (including MYSQL_DATA_TYPE added by to_columns_table_response)
        self.assertListEqual(sorted(list(response.data_frame.columns)), sorted(list(INF_SCHEMA_COLUMNS_NAMES_SET)))

        self.assertEqual(response.data_frame.iloc[0]['COLUMN_NAME'], 'COL1')
        self.assertEqual(response.data_frame.iloc[0]['DATA_TYPE'], 'VARCHAR')
        self.assertIn('MYSQL_DATA_TYPE', response.data_frame.columns)
        self.assertIsNotNone(response.data_frame.iloc[0]['MYSQL_DATA_TYPE'])

        del self.handler.native_query


if __name__ == '__main__':
    unittest.main()
