from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

import mysql.connector
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    INF_SCHEMA_COLUMNS_NAMES_SET,
    RESPONSE_TYPE
)


class TestMySQLHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='password',
            database='test_db',
            ssl=False,
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return mysql.connector.Error("Connection Failed")

    @property
    def get_tables_query(self):
        return """
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                TABLE_TYPE AS table_type
            FROM
                information_schema.TABLES
            WHERE
                TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                AND TABLE_SCHEMA = DATABASE()
            ORDER BY 2
            ;
        """

    @property
    def get_columns_query(self):
        return f"DESCRIBE `{self.mock_table}`;"

    def create_handler(self):
        return MySQLHandler('mysql', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mysql.connector.connect')

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
        mock_conn.is_connected = MagicMock(return_value=True)

        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]

        # MySQL cursor provides column info via description attribute
        mock_cursor.description = [
            ('id', None, None, None, None, None, None),
            ('name', None, None, None, None, None, None)
        ]

        mock_cursor.with_rows = True

        query_str = "SELECT * FROM test_table"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(dictionary=True, buffered=True)
        mock_cursor.execute.assert_called_once_with(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(data.data_frame, DataFrame)

        expected_columns = ['id', 'name']
        self.assertEqual(list(data.data_frame.columns), expected_columns)

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
        mock_conn.is_connected = MagicMock(return_value=True)

        mock_cursor.with_rows = False
        mock_cursor.rowcount = 1

        query_str = "INSERT INTO test_table VALUES (1, 'test')"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(dictionary=True, buffered=True)
        mock_cursor.execute.assert_called_once_with(query_str)

        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.OK)
        self.assertEqual(data.affected_rows, 1)

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
        mock_conn.is_connected = MagicMock(return_value=True)

        error_msg = "Syntax error in SQL statement"
        error = mysql.connector.Error(error_msg)
        mock_cursor.execute.side_effect = error

        query_str = "INVALID SQL"
        data = self.handler.native_query(query_str)

        mock_conn.cursor.assert_called_once_with(dictionary=True, buffered=True)
        mock_cursor.execute.assert_called_once_with(query_str)

        assert isinstance(data, Response)
        self.assertEqual(data.type, RESPONSE_TYPE.ERROR)
        self.assertEqual(data.error_message, str(error))

        mock_conn.rollback.assert_called_once()

    def test_is_connected_property(self):
        """
        Tests the is_connected property to ensure it correctly reflects the connection state
        """
        self.handler.connection = None
        self.assertFalse(self.handler.is_connected)

        mock_conn = MagicMock()
        mock_conn.is_connected = MagicMock(return_value=False)
        self.handler.connection = mock_conn
        self.assertFalse(self.handler.is_connected)

        mock_conn.is_connected = MagicMock(return_value=True)
        self.handler.connection = mock_conn
        self.assertTrue(self.handler.is_connected)

    def test_disconnect(self):
        """
        Tests the disconnect method to ensure it correctly closes connections
        """
        mock_conn = MagicMock()
        mock_conn.is_connected = MagicMock(return_value=True)
        self.handler.connection = mock_conn

        self.handler.disconnect()

        mock_conn.close.assert_called_once()

        mock_conn.reset_mock()
        mock_conn.is_connected = MagicMock(return_value=False)
        self.handler.disconnect()
        mock_conn.close.assert_not_called()

    def test_unpack_config(self):
        """
        Tests the _unpack_config method to ensure it correctly validates and unpacks connection data
        """
        with patch('mindsdb.integrations.handlers.mysql_handler.mysql_handler.ConnectionConfig') as mock_config_class:
            mock_model = MagicMock()
            mock_model.model_dump.return_value = {
                'host': '127.0.0.1',
                'port': 3306,
                'user': 'root',
                'password': 'password',
                'database': 'test_db',
            }
            mock_config_class.return_value = mock_model

            valid_config = self.dummy_connection_data.copy()
            self.handler.connection_data = valid_config

            config = self.handler._unpack_config()
            mock_config_class.assert_called_once_with(**valid_config)
            mock_model.model_dump.assert_called_once_with(exclude_unset=True)

            self.assertEqual(config['host'], '127.0.0.1')
            self.assertEqual(config['port'], 3306)
            self.assertEqual(config['user'], 'root')
            self.assertEqual(config['password'], 'password')
            self.assertEqual(config['database'], 'test_db')

            mock_config_class.side_effect = ValueError("Invalid config")
            with self.assertRaises(ValueError):
                self.handler._unpack_config()

    def test_connect_with_ssl(self):
        """
        Tests connecting with SSL configuration to ensure SSL parameters are correctly passed
        """
        self.handler.connection_data = self.dummy_connection_data.copy()
        self.handler.connection_data['ssl'] = True
        self.handler.connection_data['ssl_ca'] = '/path/to/ca.pem'
        self.handler.connection_data['ssl_cert'] = '/path/to/cert.pem'
        self.handler.connection_data['ssl_key'] = '/path/to/key.pem'

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertIn('client_flags', call_kwargs)
        self.assertIn(mysql.connector.constants.ClientFlag.SSL, call_kwargs['client_flags'])
        self.assertEqual(call_kwargs['ssl_ca'], '/path/to/ca.pem')
        self.assertEqual(call_kwargs['ssl_cert'], '/path/to/cert.pem')
        self.assertEqual(call_kwargs['ssl_key'], '/path/to/key.pem')

    def test_connect_sets_configuration(self):
        """
        Tests that connect method correctly sets default configuration values when not provided
        """
        self.handler.connection_data = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': 'password',
            'database': 'test_db'
        }

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertEqual(call_kwargs['connection_timeout'], 10)
        self.assertEqual(call_kwargs['collation'], 'utf8mb4_general_ci')
        self.assertEqual(call_kwargs['use_pure'], True)

        # Verify autocommit was set on the connection
        self.mock_connect.return_value.autocommit = True

    def test_query_method(self):
        """
        Tests the query method to ensure it correctly converts ASTNode to SQL and calls native_query
        """
        with patch('mindsdb.integrations.handlers.mysql_handler.mysql_handler.SqlalchemyRender') as mock_renderer_class:
            mock_renderer = MagicMock()
            mock_renderer.get_string.return_value = "SELECT * FROM test"
            mock_renderer_class.return_value = mock_renderer

            self.handler.native_query = MagicMock()
            self.handler.native_query.return_value = Response(RESPONSE_TYPE.OK)

            mock_ast = MagicMock()

            result = self.handler.query(mock_ast)

            mock_renderer_class.assert_called_once_with('mysql')

            mock_renderer.get_string.assert_called_once_with(mock_ast, with_failback=True)

            self.handler.native_query.assert_called_once_with("SELECT * FROM test")
            self.assertEqual(result, self.handler.native_query.return_value)

    def test_connection_with_conn_attrs(self):
        """
        Tests connecting with connection attributes to ensure they are correctly passed
        """
        self.handler.connection_data = self.dummy_connection_data.copy()
        self.handler.connection_data['conn_attrs'] = {
            'program_name': 'mindsdb',
            'client_version': '1.0'
        }

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertEqual(call_kwargs['conn_attrs'], {
            'program_name': 'mindsdb',
            'client_version': '1.0'
        })

    def test_get_tables(self):
        """
        Tests that get_tables calls native_query with the correct SQL
        """
        expected_response = Response(RESPONSE_TYPE.OK)
        self.handler.native_query = MagicMock(return_value=expected_response)

        response = self.handler.get_tables()

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]

        self.assertIn('information_schema.TABLES', call_args)
        self.assertIn('TABLE_SCHEMA', call_args)
        self.assertIn('TABLE_NAME', call_args)
        self.assertIn('TABLE_TYPE', call_args)

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
            select
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
            from
                information_schema.columns
            where
                table_name = '{table_name}';
        """
        self.assertEqual(call_args, expected_sql)
        self.assertEqual(response, expected_response)


if __name__ == '__main__':
    unittest.main()
