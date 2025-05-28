from collections import OrderedDict
import unittest
from decimal import Decimal
from unittest.mock import patch, MagicMock
from uuid import UUID
import datetime

from pymssql import OperationalError
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    INF_SCHEMA_COLUMNS_NAMES_SET,
    RESPONSE_TYPE
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


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

    def test_types_casting(self):
        """Test that types are casted correctly
        """
        query_str = "SELECT * FROM test_table"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        # region test numeric types (and bool, as bit is a synonym for boolean)
        """Data obtained using:
        CREATE TABLE test_numeric_types (
            n_bit BIT,                          -- 0|1|NULL
            n_tinyint TINYINT,                  -- 0:255
            n_smallint SMALLINT,                -- -32,768:32,767
            n_int INT,                          -- -2^31:2^31-1
            n_bigint BIGINT,                    -- -2^63:2^63-1
            n_decimal DECIMAL(18,2),
            n_decimal_p DECIMAL(38),
            n_numeric NUMERIC(18,4),
            n_money MONEY,                      -- -922,337,203,685,477.5808:922,337,203,685,477.5807
            n_smallmoney SMALLMONEY,            -- -214,748.3648:214,748.3647
            n_float FLOAT(53),
            n_real REAL                         -- FLOAT(24)
        );

        INSERT INTO test_numeric_types (
            n_bit,
            n_tinyint,
            n_smallint,
            n_int,
            n_bigint,
            n_decimal,
            n_decimal_p,
            n_numeric,
            n_money,
            n_smallmoney,
            n_float,
            n_real
        ) VALUES (
            1,                                  -- n_bit
            255,                                -- n_tinyint
            32767,                              -- n_smallint
            2147483647,                         -- n_int
            9223372036854775807,                -- n_bigint
            1234.56,                            -- n_decimal
            12345678901234567890123456789012345678, -- n_decimal_p
            1234.5678,                          -- n_numeric
            $123456.7890,                       -- n_money
            $214748.3647,                       -- n_smallmoney
            3.14159265358979,                   -- n_float
            3.141592                            -- n_real
        );
        """
        input_row = {
            'n_bit': True, 'n_tinyint': 255, 'n_smallint': 32767, 'n_int': 2147483647,
            'n_bigint': 9223372036854775807, 'n_decimal': Decimal('1234.56'),
            'n_decimal_p': Decimal('12345678901234567890123456789012345678'),
            'n_numeric': Decimal('1234.5678'), 'n_money': Decimal('123456.7890'),
            'n_smallmoney': Decimal('214748.3647'), 'n_float': 3.14159265358979,
            'n_real': 3.141592025756836
        }
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ('n_bit', 3, None, None, None, None, None),
            ('n_tinyint', 3, None, None, None, None, None),
            ('n_smallint', 3, None, None, None, None, None),
            ('n_int', 3, None, None, None, None, None),
            ('n_bigint', 3, None, None, None, None, None),
            ('n_decimal', 5, None, None, None, None, None),
            ('n_decimal_p', 5, None, None, None, None, None),
            ('n_numeric', 5, None, None, None, None, None),
            ('n_money', 5, None, None, None, None, None),
            ('n_smallmoney', 5, None, None, None, None, None),
            ('n_float', 3, None, None, None, None, None),
            ('n_real', 3, None, None, None, None, None)
        ]

        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.TINYINT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.FLOAT
        ]
        self.assertEquals(response.mysql_types, excepted_mysql_types)
        for columns_name, input_value in input_row.items():
            result_value = response.data_frame[columns_name][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # region test string types
        """Data obtained using:
        CREATE TABLE test_text_blob_types (
            t_char CHAR(10),
            t_nchar NCHAR(10),                -- Unicode
            t_varchar VARCHAR(100),
            t_nvarchar NVARCHAR(100),         -- Unicode
            t_text TEXT,
            t_ntext NTEXT,                    -- Unicode
            t_binary BINARY(10),
            t_varbinary VARBINARY(100),
            t_image IMAGE,
            t_xml XML,
            t_uniqueidentifier UNIQUEIDENTIFIER
        );

        INSERT INTO test_text_blob_types (
            t_char,
            t_nchar,
            t_varchar,
            t_nvarchar,
            t_text,
            t_ntext,
            t_binary,
            t_varbinary,
            t_image,
            t_xml,
            t_uniqueidentifier
        ) VALUES (
            'Test',         -- t_char
            N'Test',        -- t_nchar
            'Test',         -- t_varchar
            N'Test',        -- t_nvarchar
            'Test',         -- t_text
            N'Test',        -- t_ntext
            0x48656C6C6F,   -- t_binary ('Hello' hex)
            0x48656C6C6F,   -- t_varbinary ('Hello World' hex)
            0x48656C6C6F,   -- t_image ('Hello Image' hex)
            '<root><element>TestXML</element><nested><value>123</value></nested></root>', -- t_xml
            NEWID()         -- t_uniqueidentifier
        );
        """
        input_row = {
            't_char': 'Test      ',
            't_nchar': 'Test      ',
            't_varchar': 'Test',
            't_nvarchar': 'Test',
            't_text': 'Test',
            't_ntext': 'Test',
            't_binary': b'Hello\x00\x00\x00\x00\x00',
            't_varbinary': b'Hello',
            't_image': b'Hello',
            't_xml': '<root><element>TestXML</element><nested><value>123</value></nested></root>',
            't_uniqueidentifier': UUID('497b4fec-4659-431d-a146-39e76740c8a9')
        }
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ('t_char', 1, None, None, None, None, None),
            ('t_nchar', 1, None, None, None, None, None),
            ('t_varchar', 1, None, None, None, None, None),
            ('t_nvarchar', 1, None, None, None, None, None),
            ('t_text', 1, None, None, None, None, None),
            ('t_ntext', 1, None, None, None, None, None),
            ('t_binary', 2, None, None, None, None, None),
            ('t_varbinary', 2, None, None, None, None, None),
            ('t_image', 2, None, None, None, None, None),
            ('t_xml', 1, None, None, None, None, None),
            ('t_uniqueidentifier', 2, None, None, None, None, None)
        ]

        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.BINARY
        ]
        self.assertEquals(response.mysql_types, excepted_mysql_types)
        for columns_name, input_value in input_row.items():
            result_value = response.data_frame[columns_name][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # region test date types
        """Data obtained using:
        CREATE TABLE test_datetime_types (
            d_date DATE,                         -- (YYYY-MM-DD)
            d_time TIME,
            d_time_p TIME(7),
            d_smalldatetime SMALLDATETIME,
            d_datetime DATETIME,
            d_datetime2 DATETIME2,
            d_datetime2_p DATETIME2(7),
            d_datetimeoffset DATETIMEOFFSET,
            d_datetimeoffset_p DATETIMEOFFSET(7)
        );

        INSERT INTO test_datetime_types (
            d_date,
            d_time,
            d_time_p,
            d_smalldatetime,
            d_datetime,
            d_datetime2,
            d_datetime2_p,
            d_datetimeoffset,
            d_datetimeoffset_p
        ) VALUES (
            GETDATE(),                                   -- d_date
            CAST(GETDATE() AS TIME),                     -- d_time
            CAST(GETDATE() AS TIME(7)),                  -- d_time_p
            GETDATE(),                                   -- d_smalldatetime
            GETDATE(),                                   -- d_datetime
            SYSDATETIME(),                               -- d_datetime2
            SYSDATETIME(),                               -- d_datetime2_p
            SYSDATETIMEOFFSET(),                         -- d_datetimeoffset
            SYSDATETIMEOFFSET()                          -- d_datetimeoffset_p
        );
        """
        input_row = {
            'd_date': datetime.date(2025, 4, 22),
            'd_time': datetime.time(12, 30, 45, 123456),
            'd_time_p': datetime.time(12, 30, 45, 123456),
            'd_smalldatetime': datetime.datetime(2025, 4, 22, 12, 30),
            'd_datetime': datetime.datetime(2025, 4, 22, 12, 30, 45, 123456),
            'd_datetime2': datetime.datetime(2025, 4, 22, 12, 30, 45, 123456),
            'd_datetime2_p': datetime.datetime(2025, 4, 22, 12, 30, 45, 123456),
            'd_datetimeoffset': datetime.datetime(2025, 4, 22, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc),
            'd_datetimeoffset_p': datetime.datetime(2025, 4, 22, 12, 30, 45, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
        }
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ('d_date', 2, None, None, None, None, None),
            ('d_time', 2, None, None, None, None, None),
            ('d_time_p', 2, None, None, None, None, None),
            ('d_smalldatetime', 4, None, None, None, None, None),
            ('d_datetime', 4, None, None, None, None, None),
            ('d_datetime2', 2, None, None, None, None, None),
            ('d_datetime2_p', 2, None, None, None, None, None),
            ('d_datetimeoffset', 2, None, None, None, None, None),
            ('d_datetimeoffset_p', 2, None, None, None, None, None)
        ]

        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            # DATE and TIME is not possible to infer, so they are BINARY
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME
        ]
        self.assertEquals(response.mysql_types, excepted_mysql_types)
        for columns_name, input_value in input_row.items():
            result_value = response.data_frame[columns_name][0]
            if columns_name == 'd_datetimeoffset_p':
                self.assertEqual(result_value.strftime("%Y-%m-%d %H:%M:%S"), '2025-04-22 19:30:45')
                continue
            self.assertEqual(result_value, input_value)
        # endregion


if __name__ == '__main__':
    unittest.main()
