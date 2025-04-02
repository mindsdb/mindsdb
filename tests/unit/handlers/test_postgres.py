from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

import psycopg
from psycopg.pq import ExecStatus
import pandas as pd
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest, MockCursorContextManager
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class TestPostgresHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='127.0.0.1',
            port=5432,
            user='example_user',
            schema='public',
            password='example_pass',
            database='example_db',
            sslmode='prefer'
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return psycopg.Error("Connection Failed")

    @property
    def get_tables_query(self):
        return """
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('information_schema', 'pg_catalog')
                and table_type in ('BASE TABLE', 'VIEW')
                and table_schema = current_schema()
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
            AND
                table_schema = current_schema()
        """

    def create_handler(self):
        return PostgresHandler('psql', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('psycopg.connect')

    def test_native_query_command_ok(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and handles the case
        where the query doesn't return a result set (ExecStatus.COMMAND_OK)
        """
        mock_conn = MagicMock()
        # Use MockCursorContextManager for simplified mocking
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.execute.return_value = None

        # Setup pgresult
        mock_pgresult = MagicMock()
        mock_pgresult.status = ExecStatus.COMMAND_OK
        mock_cursor.pgresult = mock_pgresult

        query_str = "INSERT INTO table VALUES (1, 2, 3)"
        data = self.handler.native_query(query_str)
        mock_cursor.execute.assert_called_once_with(query_str)
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.OK)

    def test_native_query_with_results(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and handles the case
        where the query returns a result set
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.fetchall = MagicMock(return_value=[
            [1, 'name1'],
            [2, 'name2']
        ])

        # Create proper description objects with necessary type_code for _cast_dtypes
        class ColumnDescription:
            def __init__(self, name, type_code=None):
                self.name = name
                self.type_code = type_code  # Needed for the _cast_dtypes method

        mock_cursor.description = [
            ColumnDescription('id', 23),  # int4 type code
            ColumnDescription('name', 25)  # text type code
        ]

        # Make sure pgresult doesn't have COMMAND_OK status
        mock_pgresult = MagicMock()
        mock_pgresult.status = ExecStatus.TUPLES_OK
        mock_cursor.pgresult = mock_pgresult

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)
        mock_cursor.execute.assert_called_once_with(query_str)
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(data.data_frame, DataFrame)
        self.assertEqual(list(data.data_frame.columns), ['id', 'name'])

    def test_native_query_with_params(self):
        """
        Tests the `native_query` method with parameters to ensure executemany is called correctly
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_pgresult = MagicMock()
        mock_pgresult.status = ExecStatus.COMMAND_OK
        mock_cursor.pgresult = mock_pgresult

        query_str = "INSERT INTO table VALUES (%s, %s)"
        params = [(1, 'a'), (2, 'b')]
        data = self.handler.native_query(query_str, params=params)
        mock_cursor.executemany.assert_called_once_with(query_str, params)
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)

    def test_native_query_error(self):
        """
        Tests the `native_query` method to ensure it properly handles and returns database errors
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        error_msg = "Syntax error in SQL statement"
        error = psycopg.Error(error_msg)
        # Using side_effect to simulate an exception when execute is called
        mock_cursor.execute.side_effect = error

        query_str = "INVALID SQL"
        data = self.handler.native_query(query_str)

        mock_cursor.execute.assert_called_once_with(query_str)

        assert isinstance(data, Response)
        self.assertEqual(data.type, RESPONSE_TYPE.ERROR)

        # The handler implementation sets error_code to 0, check error_message instead
        self.assertEqual(data.error_code, 0)
        self.assertEqual(data.error_message, str(error))

        # Ensure rollback was called
        mock_conn.rollback.assert_called_once()

    def test_cast_dtypes(self):
        """
        Tests the _cast_dtypes method to ensure it correctly converts PostgreSQL types to pandas types
        """
        df = pd.DataFrame({
            'int2_col': ['1', '2'],
            'int4_col': ['10', '20'],
            'int8_col': ['100', '200'],
            'numeric_col': ['1.5', '2.5'],
            'float4_col': ['1.1', '2.2'],
            'float8_col': ['10.1', '20.2'],
            'text_col': ['a', 'b']
        })

        # Create a proper description class to mimic psycopg cursor description items
        class ColumnDescription:
            def __init__(self, name, type_code):
                self.name = name
                self.type_code = type_code

        # Create type code mapping
        type_codes = {
            'int2': 21,    # Typical OID for int2
            'int4': 23,    # Typical OID for int4
            'int8': 20,    # Typical OID for int8
            'numeric': 1700,  # Typical OID for numeric
            'float4': 700,  # Typical OID for float4
            'float8': 701,  # Typical OID for float8
            'text': 25     # Typical OID for text
        }

        original_get = psycopg.postgres.types.get

        try:
            type_mocks = {}
            for pg_type, oid in type_codes.items():
                type_mock = MagicMock()
                type_mock.name = pg_type
                type_mocks[oid] = type_mock

            # Mock the types.get function
            # Make it return a default mock for any OID to avoid KeyError
            def mock_get(oid):
                if oid in type_mocks:
                    return type_mocks[oid]
                else:
                    # Return a default mock with unknown type name
                    default_mock = MagicMock()
                    default_mock.name = 'unknown'
                    return default_mock

            psycopg.postgres.types.get = mock_get

            description = [
                ColumnDescription('int2_col', type_codes['int2']),
                ColumnDescription('int4_col', type_codes['int4']),
                ColumnDescription('int8_col', type_codes['int8']),
                ColumnDescription('numeric_col', type_codes['numeric']),
                ColumnDescription('float4_col', type_codes['float4']),
                ColumnDescription('float8_col', type_codes['float8']),
                ColumnDescription('text_col', type_codes['text'])
            ]

            self.handler._cast_dtypes(df, description)
            # Verify the types were correctly cast
            self.assertEqual(df['int2_col'].dtype, 'int16')
            self.assertEqual(df['int4_col'].dtype, 'int32')
            self.assertEqual(df['int8_col'].dtype, 'int64')
            self.assertEqual(df['numeric_col'].dtype, 'float64')
            self.assertEqual(df['float4_col'].dtype, 'float32')
            self.assertEqual(df['float8_col'].dtype, 'float64')
            self.assertEqual(df['text_col'].dtype, 'object')

        finally:
            # Restore original function
            psycopg.postgres.types.get = original_get

    def test_cast_dtypes_with_nulls(self):
        """
        Tests the _cast_dtypes method with NULL values to ensure correct handling
        """
        df = pd.DataFrame({
            'int2_col': ['1', None],
            'float4_col': ['1.1', None]
        })

        # Create a proper description class to mimic psycopg cursor description items
        class ColumnDescription:
            def __init__(self, name, type_code):
                self.name = name
                self.type_code = type_code

        # Create type code mapping
        type_codes = {
            'int2': 21,    # Typical OID for int2
            'float4': 700,  # Typical OID for float4
        }

        # Create mock psycopg.postgres.types.get function
        original_get = psycopg.postgres.types.get

        try:
            type_mocks = {}
            for pg_type, oid in type_codes.items():
                type_mock = MagicMock()
                type_mock.name = pg_type
                type_mocks[oid] = type_mock

            # Make it return a default mock for any OID to avoid KeyError
            def mock_get(oid):
                if oid in type_mocks:
                    return type_mocks[oid]
                else:
                    default_mock = MagicMock()
                    default_mock.name = 'unknown'
                    return default_mock

            psycopg.postgres.types.get = mock_get

            # Set up description with our custom class
            description = [
                ColumnDescription('int2_col', type_codes['int2']),
                ColumnDescription('float4_col', type_codes['float4'])
            ]

            self.handler._cast_dtypes(df, description)

            self.assertEqual(df['int2_col'].dtype, 'int16')
            self.assertEqual(df['float4_col'].dtype, 'float32')
            self.assertEqual(df['int2_col'].iloc[1], 0)
            self.assertEqual(df['float4_col'].iloc[1], 0)

        finally:
            psycopg.postgres.types.get = original_get

    def test_insert(self):
        """
        Tests the insert method to ensure it correctly uses the COPY command
        to insert a DataFrame into a PostgreSQL table
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        # Create mock for copy operation
        copy_obj = MagicMock()
        mock_cursor.copy = MagicMock(return_value=copy_obj)
        # Ensure copy.__enter__ returns the copy object to mimic context manager
        copy_obj.__enter__ = MagicMock(return_value=copy_obj)
        copy_obj.__exit__ = MagicMock(return_value=None)

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c']
        })

        self.handler.insert('test_table', df)

        # Verify copy was called with correct SQL
        copy_sql = 'copy "test_table" ("id","name") from STDIN  WITH CSV'
        mock_cursor.copy.assert_called_once_with(copy_sql)
        mock_conn.commit.assert_called_once()

    def test_insert_error(self):
        """
        Tests the insert method to ensure it correctly handles errors
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        error_msg = "Table doesn't exist"
        error = psycopg.Error(error_msg)
        mock_cursor.copy = MagicMock(side_effect=error)

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c']
        })

        # Call the insert method and expect an exception
        with self.assertRaises(psycopg.Error):
            self.handler.insert('nonexistent_table', df)

        mock_conn.rollback.assert_called_once()

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
        mock_conn.reset_mock()
        self.handler.disconnect()
        mock_conn.close.assert_not_called()

    def test_connection_parameters(self):
        """
        Tests that connection parameters are correctly passed to psycopg.connect
        """
        self.tearDown()
        self.setUp()
        self.handler.connection_args['connection_parameters'] = {
            'application_name': 'mindsdb_test',
            'keepalives': 1
        }

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]

        self.assertEqual(call_kwargs['application_name'], 'mindsdb_test')
        self.assertEqual(call_kwargs['keepalives'], 1)
        self.assertEqual(call_kwargs['connect_timeout'], 10)
        self.assertEqual(call_kwargs['sslmode'], 'prefer')

        expected_options = '-c search_path=public,public'
        self.assertEqual(call_kwargs['options'], expected_options)

        # Test with a different schema
        # Create a fresh handler with different schema
        self.tearDown()
        self.setUp()
        self.handler.connection_args['schema'] = 'custom_schema'
        self.handler.connection_args['connection_parameters'] = {
            'application_name': 'mindsdb_test'
        }

        self.handler.connect()
        call_kwargs = self.mock_connect.call_args[1]
        expected_options = '-c search_path=custom_schema,public'
        self.assertEqual(call_kwargs['options'], expected_options)


if __name__ == '__main__':
    unittest.main()
