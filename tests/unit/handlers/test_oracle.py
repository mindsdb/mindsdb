from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from oracledb import DatabaseError
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.oracle_handler.oracle_handler import OracleHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    INF_SCHEMA_COLUMNS_NAMES_SET,
    RESPONSE_TYPE
)


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

    def test_connect_validation(self):
        """
        Tests that connect method raises ValueError when required connection parameters are missing
        """
        # Test missing 'user'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['user']
        handler = OracleHandler('oracle', connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'password'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['password']
        handler = OracleHandler('oracle', connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'dsn' AND missing 'host'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['dsn']
        invalid_connection_args.pop('host', None)
        handler = OracleHandler('oracle', connection_data=invalid_connection_args)
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
        Tests the check_connection method to ensure it properly tests connectivity using ping()
        """
        mock_conn = MagicMock()
        self.handler.connect = MagicMock(return_value=mock_conn)

        response = self.handler.check_connection()
        mock_conn.ping.assert_called_once()
        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)

        self.handler.connect = MagicMock()
        connect_error = DatabaseError("Connection error")
        self.handler.connect.side_effect = connect_error
        response = self.handler.check_connection()
        self.assertFalse(response.success)
        self.assertEqual(response.error_message, str(connect_error))
        self.handler.connect.assert_called_once()

        mock_conn.reset_mock()
        self.handler.connect = MagicMock(return_value=mock_conn)
        ping_error = DatabaseError("Ping error")
        mock_conn.ping.side_effect = ping_error
        response = self.handler.check_connection()
        self.assertFalse(response.success)
        self.assertEqual(response.error_message, str(ping_error))
        mock_conn.ping.assert_called_once()

    def test_native_query_with_results(self):
        """
        Tests the `native_query` method for a SELECT statement returning results.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.fetchall.return_value = [
            (1, 'test1'),
            (2, 'test2')
        ]
        mock_cursor.description = [
            ('ID', None, None, None, None, None, None),
            ('NAME', None, None, None, None, None, None)
        ]

        query_str = "SELECT ID, NAME FROM test_table"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query_str)
        mock_cursor.fetchall.assert_called_once()
        mock_conn.commit.assert_called_once()

        self.assertIsInstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(data.data_frame, DataFrame)
        expected_columns = ['ID', 'NAME']
        self.assertListEqual(list(data.data_frame.columns), expected_columns)
        self.assertEqual(len(data.data_frame), 2)

    def test_native_query_no_results(self):
        """
        Tests the `native_query` method for a statement that doesn't return results (e.g., INSERT).
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.description = None
        mock_cursor.rowcount = 1

        query_str = "INSERT INTO test_table VALUES (1, 'test')"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query_str)
        mock_cursor.fetchall.assert_not_called()
        mock_conn.commit.assert_called_once()

        self.assertIsInstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.OK)
        self.assertEqual(data.affected_rows, 1)

    def test_native_query_error(self):
        """
        Tests the `native_query` method handles database errors correctly.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        error_msg = "ORA-00942: table or view does not exist"
        error = DatabaseError(error_msg)
        mock_cursor.execute.side_effect = error

        query_str = "INVALID SQL"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query_str)
        mock_cursor.fetchall.assert_not_called()
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

        self.assertIsInstance(data, Response)
        self.assertEqual(data.type, RESPONSE_TYPE.ERROR)
        self.assertEqual(data.error_message, error_msg)

    def test_query_method(self):
        """
        Tests the query method to ensure it correctly converts ASTNode to SQL and calls native_query.
        """
        orig_renderer_attr = hasattr(self.handler, 'renderer')
        if orig_renderer_attr:
            orig_renderer = self.handler.renderer

        self.handler.native_query = MagicMock()
        expected_response = Response(RESPONSE_TYPE.TABLE)
        self.handler.native_query.return_value = expected_response
        mock_ast = MagicMock()

        expected_sql = "SELECT * FROM rendered_table"

        with patch('mindsdb.integrations.handlers.oracle_handler.oracle_handler.SqlalchemyRender') as MockRenderer:
            mock_renderer_instance = MockRenderer.return_value
            mock_renderer_instance.get_string.return_value = expected_sql

            result = self.handler.query(mock_ast)

            MockRenderer.assert_called_once_with("oracle")
            mock_renderer_instance.get_string.assert_called_once_with(mock_ast, with_failback=True)
            self.handler.native_query.assert_called_once_with(expected_sql)
            self.assertEqual(result, expected_response)

        del self.handler.native_query
        if orig_renderer_attr:
            self.handler.renderer = orig_renderer

    def test_get_tables(self):
        """
        Tests that get_tables calls native_query with the correct SQL for Oracle
        and returns the expected DataFrame structure.
        """
        expected_df = DataFrame([('TABLE1',), ('TABLE2',)], columns=['TABLE_NAME'])
        expected_response = Response(RESPONSE_TYPE.TABLE, data_frame=expected_df)

        self.handler.native_query = MagicMock(return_value=expected_response)

        response = self.handler.get_tables()

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]

        self.assertIn('FROM user_tables', call_args)
        self.assertIn('SELECT table_name', call_args)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)
        self.assertListEqual(list(response.data_frame.columns), ['TABLE_NAME'])
        self.assertEqual(len(response.data_frame), 2)

        del self.handler.native_query

    def test_get_columns(self):
        """
        Tests that get_columns calls native_query with the correct SQL for Oracle
        and returns the expected DataFrame structure.
        """
        query_columns = [
            'COLUMN_NAME', 'DATA_TYPE', 'ORDINAL_POSITION', 'COLUMN_DEFAULT', 'IS_NULLABLE',
            'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION',
            'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME'
        ]

        expected_df_data = [
            {
                'COLUMN_NAME': 'COL1', 'DATA_TYPE': 'VARCHAR2', 'ORDINAL_POSITION': 1, 'COLUMN_DEFAULT': None,
                'IS_NULLABLE': 'YES', 'CHARACTER_MAXIMUM_LENGTH': 255, 'CHARACTER_OCTET_LENGTH': None,
                'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None, 'DATETIME_PRECISION': None,
                'CHARACTER_SET_NAME': 'AL32UTF8', 'COLLATION_NAME': None
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
        self.assertIn('FROM USER_TAB_COLUMNS', call_args)
        self.assertIn(f"WHERE table_name = '{table_name}'", call_args)
        self.assertIn('COLUMN_NAME', call_args)
        self.assertIn('DATA_TYPE', call_args)
        self.assertIn('COLUMN_ID AS ORDINAL_POSITION', call_args)
        self.assertNotIn('MYSQL_DATA_TYPE', call_args)

        self.assertEqual(response.type, RESPONSE_TYPE.COLUMNS_TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)

        expected_final_columns = INF_SCHEMA_COLUMNS_NAMES_SET
        self.assertSetEqual(set(response.data_frame.columns), expected_final_columns)

        self.assertEqual(response.data_frame.iloc[0]['COLUMN_NAME'], 'COL1')
        self.assertEqual(response.data_frame.iloc[0]['DATA_TYPE'], 'VARCHAR2')
        self.assertIn('MYSQL_DATA_TYPE', response.data_frame.columns)
        self.assertIsNotNone(response.data_frame.iloc[0]['MYSQL_DATA_TYPE'])

        del self.handler.native_query


if __name__ == '__main__':
    unittest.main()
