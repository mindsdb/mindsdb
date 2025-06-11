import pytest
import unittest
import datetime
from array import array
from decimal import Decimal
from collections import OrderedDict
from unittest.mock import patch, MagicMock

try:
    import oracledb
    from oracledb import DatabaseError
    from mindsdb.integrations.handlers.oracle_handler.oracle_handler import OracleHandler
except ImportError:
    pytestmark = pytest.mark.skip("Oracle handler not installed")

import pandas as pd
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.libs.response import HandlerResponse as Response, INF_SCHEMA_COLUMNS_NAMES_SET, RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


class TestOracleHandler(BaseDatabaseHandlerTest, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(user="example_user", password="example_pass", dsn="example_dsn")

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
        return OracleHandler("oracle", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.oracle_handler.oracle_handler.connect")

    def test_connect_validation(self):
        """
        Tests that connect method raises ValueError when required connection parameters are missing
        """
        # Test missing 'user'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args["user"]
        handler = OracleHandler("oracle", connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'password'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args["password"]
        handler = OracleHandler("oracle", connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'dsn' AND missing 'host'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args["dsn"]
        invalid_connection_args.pop("host", None)
        handler = OracleHandler("oracle", connection_data=invalid_connection_args)
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

        mock_cursor.fetchall.return_value = [(1, "test1"), (2, "test2")]
        mock_cursor.description = [
            ("ID", None, None, None, None, None, None),
            ("NAME", None, None, None, None, None, None),
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
        expected_columns = ["ID", "NAME"]
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
        orig_renderer_attr = hasattr(self.handler, "renderer")
        if orig_renderer_attr:
            orig_renderer = self.handler.renderer

        self.handler.native_query = MagicMock()
        expected_response = Response(RESPONSE_TYPE.TABLE)
        self.handler.native_query.return_value = expected_response
        mock_ast = MagicMock()

        expected_sql = "SELECT * FROM rendered_table"

        with patch("mindsdb.integrations.handlers.oracle_handler.oracle_handler.SqlalchemyRender") as MockRenderer:
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
        expected_df = DataFrame([("TABLE1",), ("TABLE2",)], columns=["TABLE_NAME"])
        expected_response = Response(RESPONSE_TYPE.TABLE, data_frame=expected_df)

        self.handler.native_query = MagicMock(return_value=expected_response)

        response = self.handler.get_tables()

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]

        self.assertIn("FROM user_tables", call_args)
        self.assertIn("SELECT table_name", call_args)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)
        self.assertListEqual(list(response.data_frame.columns), ["TABLE_NAME"])
        self.assertEqual(len(response.data_frame), 2)

        del self.handler.native_query

    def test_get_columns(self):
        """
        Tests that get_columns calls native_query with the correct SQL for Oracle
        and returns the expected DataFrame structure.
        """
        query_columns = [
            "COLUMN_NAME",
            "DATA_TYPE",
            "ORDINAL_POSITION",
            "COLUMN_DEFAULT",
            "IS_NULLABLE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
        ]

        expected_df_data = [
            {
                "COLUMN_NAME": "COL1",
                "DATA_TYPE": "VARCHAR2",
                "ORDINAL_POSITION": 1,
                "COLUMN_DEFAULT": None,
                "IS_NULLABLE": "YES",
                "CHARACTER_MAXIMUM_LENGTH": 255,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "DATETIME_PRECISION": None,
                "CHARACTER_SET_NAME": "AL32UTF8",
                "COLLATION_NAME": None,
            },
            {
                "COLUMN_NAME": "COL2",
                "DATA_TYPE": "NUMBER",
                "ORDINAL_POSITION": 2,
                "COLUMN_DEFAULT": "0",
                "IS_NULLABLE": "NO",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": 38,
                "NUMERIC_SCALE": 0,
                "DATETIME_PRECISION": None,
                "CHARACTER_SET_NAME": None,
                "COLLATION_NAME": None,
            },
        ]
        expected_df = DataFrame(expected_df_data, columns=query_columns)

        expected_response = Response(RESPONSE_TYPE.TABLE, data_frame=expected_df)
        self.handler.native_query = MagicMock(return_value=expected_response)

        table_name = "test_table"
        response = self.handler.get_columns(table_name)

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]
        self.assertIn("FROM USER_TAB_COLUMNS", call_args)
        self.assertIn(f"WHERE table_name = '{table_name}'", call_args)
        self.assertIn("COLUMN_NAME", call_args)
        self.assertIn("DATA_TYPE", call_args)
        self.assertIn("COLUMN_ID AS ORDINAL_POSITION", call_args)
        self.assertNotIn("MYSQL_DATA_TYPE", call_args)

        self.assertEqual(response.type, RESPONSE_TYPE.COLUMNS_TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)

        expected_final_columns = INF_SCHEMA_COLUMNS_NAMES_SET
        self.assertSetEqual(set(response.data_frame.columns), expected_final_columns)

        self.assertEqual(response.data_frame.iloc[0]["COLUMN_NAME"], "COL1")
        self.assertEqual(response.data_frame.iloc[0]["DATA_TYPE"], "VARCHAR2")
        self.assertIn("MYSQL_DATA_TYPE", response.data_frame.columns)
        self.assertIsNotNone(response.data_frame.iloc[0]["MYSQL_DATA_TYPE"])

        del self.handler.native_query

    def test_types_casting(self):
        """Test that types are casted correctly"""
        query_str = "SELECT * FROM test_table"
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        # region test numeric types
        """Data obtained using:
        CREATE TABLE test_numeric_types (
            n_number NUMBER,
            n_number_p NUMBER(38),
            n_number_ps NUMBER(10,2),
            n_integer INTEGER,
            n_smallint SMALLINT,
            n_decimal DECIMAL(10,2),
            n_decimal_p DECIMAL(15),
            n_numeric NUMERIC(10,2),
            n_float FLOAT,
            n_float_p FLOAT(126),
            n_real REAL,                         -- is FLOAT(63)
            n_double_precision DOUBLE PRECISION, -- is FLOAT(126)
            n_binary_float BINARY_FLOAT,        -- 32-bit
            n_binary_double BINARY_DOUBLE       -- 64-bit
        );

        INSERT INTO test_numeric_types (
            n_number,
            n_number_p,
            n_number_ps,
            n_integer,
            n_smallint,
            n_decimal,
            n_decimal_p,
            n_numeric,
            n_float,
            n_float_p,
            n_real,
            n_double_precision,
            n_binary_float,
            n_binary_double
        ) VALUES (
            123456.789,                       -- n_number
            12345678901234567890123456789012345678, -- n_number_p (38 digits)
            1234.56,                          -- n_number_ps
            2147483647,                       -- n_int
            32767,                            -- n_smallint
            9876.54,                          -- n_decimal
            123456789012345,                  -- n_decimal_p
            1234.56,                          -- n_numeric
            3.14159265358979,                 -- n_float
            2.718281828459045235360287471352, -- n_float_p
            3.14159,                          -- n_real
            2.7182818284590452,               -- n_double_precision
            3.14159265E0,                     -- n_binary_float
            2.718281828459045235360287471352E0 -- n_binary_double
        );
        """
        input_row = (
            123456.789,
            12345678901234567890123456789012345678,
            1234.56,
            2147483647,
            32767,
            9876.54,
            123456789012345,
            1234.56,
            3.14159265358979,
            2.718281828459045,
            3.14159,
            2.718281828459045,
            3.1415927410125732,
            2.718281828459045,
        )
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ("N_NUMBER", oracledb.DB_TYPE_NUMBER, 127, None, 0, -127, True),
            ("N_NUMBER_P", oracledb.DB_TYPE_NUMBER, 39, None, 38, 0, True),
            ("N_NUMBER_PS", oracledb.DB_TYPE_NUMBER, 14, None, 10, 2, True),
            ("N_INTEGER", oracledb.DB_TYPE_NUMBER, 39, None, 38, 0, True),
            ("N_SMALLINT", oracledb.DB_TYPE_NUMBER, 39, None, 38, 0, True),
            ("N_DECIMAL", oracledb.DB_TYPE_NUMBER, 14, None, 10, 2, True),
            ("N_DECIMAL_P", oracledb.DB_TYPE_NUMBER, 16, None, 15, 0, True),
            ("N_NUMERIC", oracledb.DB_TYPE_NUMBER, 14, None, 10, 2, True),
            ("N_FLOAT", oracledb.DB_TYPE_NUMBER, 127, None, 126, -127, True),
            ("N_FLOAT_P", oracledb.DB_TYPE_NUMBER, 127, None, 126, -127, True),
            ("N_REAL", oracledb.DB_TYPE_NUMBER, 64, None, 63, -127, True),
            ("N_DOUBLE_PRECISION", oracledb.DB_TYPE_NUMBER, 127, None, 126, -127, True),
            ("N_BINARY_FLOAT", oracledb.DB_TYPE_NUMBER, 127, None, None, None, True),
            ("N_BINARY_DOUBLE", oracledb.DB_TYPE_NUMBER, 127, None, None, None, True),
        ]

        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.DECIMAL,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.FLOAT,
        ]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[response.data_frame.columns[i]][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # region rest boolean types
        """Data obtained using:
        CREATE TABLE test_boolean_test (
            t_boolean boolean,
            t_bool bool
        );

        INSERT INTO test_boolean_test (t_boolean, t_bool) VALUES (TRUE, false);
        """

        input_row = (True, False)
        mock_cursor.fetchall.return_value = [input_row]
        mock_cursor.description = [
            ("T_BOOLEAN", oracledb.DB_TYPE_BOOLEAN, None, None, None, None, True),
            ("T_BOOL", oracledb.DB_TYPE_BOOLEAN, None, None, None, None, True),
        ]
        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [MYSQL_DATA_TYPE.BOOLEAN, MYSQL_DATA_TYPE.BOOLEAN]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[response.data_frame.columns[i]][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # region test text types
        """Data obtained using:
        CREATE TABLE test_text_types (
            t_char CHAR(10),
            t_nchar NCHAR(10),          -- unicode
            t_varchar2 VARCHAR2(100),
            t_nvarchar2 NVARCHAR2(100), -- unicode
            t_long LONG,
            t_clob CLOB,
            t_nclob NCLOB,              -- unicode
            t_raw RAW(100),
            t_blob BLOB
        );

        INSERT INTO test_text_types (
            t_char,
            t_nchar,
            t_varchar2,
            t_nvarchar2,
            t_long,
            t_clob,
            t_nclob,
            t_raw,
            t_blob
        ) VALUES (
            'Test',             -- t_char
            N'Unicode',         -- t_nchar
            'Test',             -- t_varchar2
            N'Unicode',         -- t_nvarchar2
            'Test',             -- t_long
            TO_CLOB('Test'),    -- t_clob
            TO_NCLOB('Test'),   -- t_nclob
            HEXTORAW('54657374'),     -- t_raw
            HEXTORAW('54657374')      -- t_blob
        );
        """
        input_row = ("Test      ", "Unicode   ", "Test", "Unicode", "Test", "Test", "Test", b"Test", b"Test")
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ("T_CHAR", oracledb.DB_TYPE_CHAR, 10, 10, None, None, True),
            ("T_NCHAR", oracledb.DB_TYPE_NCHAR, 10, 20, None, None, True),
            ("T_VARCHAR2", oracledb.DB_TYPE_VARCHAR, 100, 100, None, None, True),
            ("T_NVARCHAR2", oracledb.DB_TYPE_NVARCHAR, 100, 200, None, None, True),
            ("T_LONG", oracledb.DB_TYPE_LONG, None, None, None, None, True),
            ("T_CLOB", oracledb.DB_TYPE_LONG, None, None, None, None, True),
            ("T_NCLOB", oracledb.DB_TYPE_LONG_NVARCHAR, None, None, None, None, True),
            ("T_RAW", oracledb.DB_TYPE_RAW, 100, 100, None, None, True),
            ("T_BLOB", oracledb.DB_TYPE_LONG_RAW, None, None, None, None, True),
        ]
        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.BINARY,
        ]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[response.data_frame.columns[i]][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # region test date types
        """Data obtained using:
        CREATE TABLE test_datetime_types (
            d_date DATE,
            d_timestamp TIMESTAMP,
            d_timestamp_p TIMESTAMP(9)
            -- timezone is not supported in thin mode
            -- d_timestamp_tz TIMESTAMP WITH TIME ZONE,
            -- d_timestamp_tz_p TIMESTAMP(6) WITH TIME ZONE,
            -- d_timestamp_ltz TIMESTAMP WITH LOCAL TIME ZONE
        );

        INSERT INTO test_datetime_types (
            d_date,
            d_timestamp,
            d_timestamp_p
            -- timezone is not supported in thin mode
            -- d_timestamp_tz,
            -- d_timestamp_tz_p,
            -- d_timestamp_ltz
        ) VALUES (
            DATE '2023-10-15',                                                -- d_date
            TIMESTAMP '2023-10-15 10:30:45.123456789',                        -- d_timestamp
            TIMESTAMP '2023-10-15 10:30:45.123456789'                         -- d_timestamp_p
            -- timezone is not supported in thin mode
            -- TIMESTAMP '2023-10-15 10:30:45.123456' AT TIME ZONE 'America/Los_Angeles', -- d_timestamp_tz
            -- TIMESTAMP '2023-10-15 10:30:45.123456' AT TIME ZONE '-07:00',     -- d_timestamp_tz_p
            -- TIMESTAMP '2023-10-15 10:30:45.123456'                            -- d_timestamp_ltz
        );
        """
        input_row = (
            datetime.datetime(2023, 10, 15, 0, 0),
            datetime.datetime(2023, 10, 15, 10, 30, 45, 123457),
            datetime.datetime(2023, 10, 15, 10, 30, 45, 123456),
        )
        mock_cursor.fetchall.return_value = [input_row]
        mock_cursor.description = [
            ("D_DATE", oracledb.DB_TYPE_DATE, 23, None, None, None, True),
            ("D_TIMESTAMP", oracledb.DB_TYPE_TIMESTAMP, 23, None, 0, 6, True),
            ("D_TIMESTAMP_P", oracledb.DB_TYPE_TIMESTAMP, 23, None, 0, 9, True),
        ]
        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [MYSQL_DATA_TYPE.DATE, MYSQL_DATA_TYPE.TIMESTAMP, MYSQL_DATA_TYPE.TIMESTAMP]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[response.data_frame.columns[i]][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # region test nullable types
        bigint_val = 9223372036854775807
        input_rows = [(bigint_val, True), (None, None)]
        mock_cursor.fetchall.return_value = input_rows
        mock_cursor.description = [
            ("N_BIGINT", oracledb.DB_TYPE_NUMBER, 39, None, 17, 0, True),  # set 17 just to force cast to Int64
            ("T_BOOLEAN", oracledb.DB_TYPE_BOOLEAN, None, None, None, None, True),
        ]
        response: Response = self.handler.native_query(query_str)
        self.assertEqual(response.data_frame.dtypes[0], "Int64")
        self.assertEqual(response.data_frame.dtypes[1], "boolean")
        self.assertEqual(response.data_frame.iloc[0, 0], bigint_val)
        self.assertEqual(response.data_frame.iloc[0, 1], True)
        self.assertTrue(response.data_frame.iloc[1, 0] is pd.NA)
        self.assertTrue(response.data_frame.iloc[1, 1] is pd.NA)
        # endregion

        # region test vector and json type
        """Data obtained using:
            CREATE TABLE test_vector_type (
                t_embedding VECTOR(3, FLOAT32),
                t_json json
            ) TABLESPACE USERS;

            INSERT INTO test_vector_type VALUES (
                TO_VECTOR('[1.1, 2.2, 3.3]', 3, FLOAT32),
                JSON_OBJECT(
                    'category' VALUE 'electronics',
                    'price' VALUE 299.99
                )
            );
        """
        input_row = (array("f", [1.1, 2.2, 3.3]), {"category": "electronics", "price": Decimal("299.99")})
        mock_cursor.fetchall.return_value = [input_row]
        mock_cursor.description = [
            ("T_EMBEDDING", oracledb.DB_TYPE_VECTOR, None, None, None, None, True),
            ("T_JSON", oracledb.DB_TYPE_JSON, None, None, None, None, True),
        ]
        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [MYSQL_DATA_TYPE.VECTOR, MYSQL_DATA_TYPE.JSON]
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[response.data_frame.columns[i]][0]
            self.assertEqual(result_value, input_value)
        # endreion


if __name__ == "__main__":
    unittest.main()
