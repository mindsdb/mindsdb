from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from pymssql import OperationalError
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    INF_SCHEMA_COLUMNS_NAMES_SET,
    RESPONSE_TYPE
)


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
        return OperationalError("Connection Failed")

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

    def test_native_query_with_results(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and handles the case
        where the query returns a result set
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]

        mock_cursor.description = [
            ('id', None, None, None, None, None, None),
            ('name', None, None, None, None, None, None)
        ]

        query_str = "SELECT * FROM test_table"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(as_dict=True)
        mock_cursor.execute.assert_called_once_with(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(data.data_frame, DataFrame)
        expected_columns = ['id', 'name']
        self.assertEqual(list(data.data_frame.columns), expected_columns)

        mock_conn.commit.assert_called_once()

    def test_native_query_no_results(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and handles the case
        where the query doesn't return any results (e.g., INSERT, UPDATE, DELETE)
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.description = None

        query_str = "INSERT INTO test_table VALUES (1, 'test')"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(as_dict=True)
        mock_cursor.execute.assert_called_once_with(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.OK)

        mock_conn.commit.assert_called_once()

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
        error = OperationalError(error_msg)
        mock_cursor.execute.side_effect = error

        query_str = "INVALID SQL"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(as_dict=True)
        mock_cursor.execute.assert_called_once_with(query_str)

        assert isinstance(data, Response)
        self.assertEqual(data.type, RESPONSE_TYPE.ERROR)
        self.assertEqual(data.error_message, str(error))

        mock_conn.rollback.assert_called_once()

    def test_query_method(self):
        """
        Tests the query method to ensure it correctly converts ASTNode to SQL and calls native_query
        """
        orig_renderer = self.handler.renderer
        renderer_mock = MagicMock()
        renderer_mock.get_string = MagicMock(return_value="SELECT * FROM test")

        try:
            self.handler.renderer = renderer_mock
            self.handler.native_query = MagicMock()
            self.handler.native_query.return_value = Response(RESPONSE_TYPE.OK)

            mock_ast = MagicMock()
            result = self.handler.query(mock_ast)
            renderer_mock.get_string.assert_called_once_with(mock_ast, with_failback=True)
            self.handler.native_query.assert_called_once_with("SELECT * FROM test")
            self.assertEqual(result, self.handler.native_query.return_value)
        finally:
            self.handler.renderer = orig_renderer

    def test_get_tables(self):
        """
        Tests that get_tables calls native_query with the correct SQL
        """
        expected_response = Response(RESPONSE_TYPE.OK)
        self.handler.native_query = MagicMock(return_value=expected_response)

        response = self.handler.get_tables()

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]
        database = self.handler.connection_args['database']

        self.assertIn(f'{database}.INFORMATION_SCHEMA.TABLES', call_args)
        self.assertIn('table_schema', call_args)
        self.assertIn('table_name', call_args)
        self.assertIn('table_type', call_args)
        self.assertEqual(response, expected_response)

    def test_get_columns(self):
        """
        Tests that get_columns calls native_query with the correct SQL
        """
        expected_response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=DataFrame([], columns=list(INF_SCHEMA_COLUMNS_NAMES_SET))
        )
        self.handler.native_query = MagicMock(return_value=expected_response)

        table_name = "test_table"
        response = self.handler.get_columns(table_name)
        assert response.type == RESPONSE_TYPE.COLUMNS_TABLE
        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]

        expected_sql = f"""
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
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        self.assertEqual(call_args, expected_sql)
        self.assertEqual(response, expected_response)

    def test_connect_validation(self):
        """
        Tests that connect method raises ValueError when required connection parameters are missing
        """
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['host']
        handler = SqlServerHandler('mssql', connection_data=invalid_connection_args)

        with self.assertRaises(ValueError):
            handler.connect()

        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args['user']
        handler = SqlServerHandler('mssql', connection_data=invalid_connection_args)

        with self.assertRaises(ValueError):
            handler.connect()

    def test_connect_optional_params(self):
        """
        Tests that connect method passes optional parameters to the connection
        """
        self.handler.connection_args['server'] = 'my_server'
        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertEqual(call_kwargs['server'], 'my_server')
        self.tearDown()
        self.setUp()
        self.handler.connection_args['port'] = 1433
        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertEqual(call_kwargs['port'], 1433)

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
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        response = self.handler.check_connection()
        mock_cursor.execute.assert_called_once_with('select 1;')

        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)
        self.handler.connect.side_effect = OperationalError("Connection error")

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertEqual(response.error_message, "Connection error")


if __name__ == '__main__':
    unittest.main()
