import pytest

try:
    import snowflake
    import snowflake.connector
    from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler
except ImportError:
    pytestmark = pytest.mark.skip("Snowflake handler not installed")

import unittest
from unittest.mock import patch, MagicMock
from collections import OrderedDict
from decimal import Decimal
import datetime
import numpy as np
import pandas as pd
from pandas import DataFrame

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.libs.response import HandlerResponse as Response, INF_SCHEMA_COLUMNS_NAMES_SET, RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


class ColumnDescription:
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])


class TestSnowflakeHandler(BaseDatabaseHandlerTest, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            account="tvuibdy-vm85921",
            user="example_user",
            password="example_pass",
            database="example_db",
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
        return SnowflakeHandler("snowflake", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("snowflake.connector.connect")

    def test_connect_validation(self):
        """
        Tests that connect method raises ValueError when required connection parameters are missing
        """
        # Test missing 'account'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args["account"]
        handler = SnowflakeHandler("snowflake", connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'user'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args["user"]
        handler = SnowflakeHandler("snowflake", connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'password'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args["password"]
        handler = SnowflakeHandler("snowflake", connection_data=invalid_connection_args)
        with self.assertRaises(ValueError):
            handler.connect()

        # Test missing 'database'
        invalid_connection_args = self.dummy_connection_data.copy()
        del invalid_connection_args["database"]
        handler = SnowflakeHandler("snowflake", connection_data=invalid_connection_args)
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
        mock_cursor.execute.assert_called_once_with("select 1;")
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

        expected_columns = ["ID", "NAME"]
        batch1_data = [(1, "test1")]
        batch2_data = [(2, "test2")]
        mock_df_batch1 = DataFrame(batch1_data, columns=expected_columns)
        mock_df_batch2 = DataFrame(batch2_data, columns=expected_columns)
        mock_cursor.fetch_pandas_batches.return_value = iter([mock_df_batch1, mock_df_batch2])

        mock_cursor.description = [
            ColumnDescription(name="ID", type_code=snowflake.connector.constants.FIELD_NAME_TO_ID["FIXED"]),
            ColumnDescription(name="NAME", type_code=snowflake.connector.constants.FIELD_NAME_TO_ID["TEXT"]),
        ]
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
        self.assertEqual(data.data_frame.iloc[0]["ID"], 1)
        self.assertEqual(data.data_frame.iloc[0]["NAME"], "test1")
        self.assertEqual(data.data_frame.iloc[1]["ID"], 2)
        self.assertEqual(data.data_frame.iloc[1]["NAME"], "test2")

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
        mock_cursor.fetchall.return_value = [{"number of rows inserted": 1}]
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
        orig_renderer = getattr(self.handler, "renderer", None)
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
            data_frame=DataFrame(
                [("table1", "SCHEMA1", "BASE TABLE")], columns=["TABLE_NAME", "TABLE_SCHEMA", "TABLE_TYPE"]
            ),
        )
        self.handler.native_query = MagicMock(return_value=expected_response)

        response = self.handler.get_tables()

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]

        self.assertIn("FROM INFORMATION_SCHEMA.TABLES", call_args)
        self.assertIn("TABLE_NAME", call_args)
        self.assertIn("TABLE_SCHEMA", call_args)
        self.assertIn("TABLE_TYPE", call_args)
        self.assertIn("current_schema()", call_args)
        self.assertIn("('BASE TABLE', 'VIEW')", call_args)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)
        self.assertListEqual(list(response.data_frame.columns), ["TABLE_NAME", "TABLE_SCHEMA", "TABLE_TYPE"])

        del self.handler.native_query

    def test_get_columns(self):
        """
        Tests that get_columns calls native_query with the correct SQL for Snowflake
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
                "DATA_TYPE": "VARCHAR",
                "ORDINAL_POSITION": 1,
                "COLUMN_DEFAULT": None,
                "IS_NULLABLE": "YES",
                "CHARACTER_MAXIMUM_LENGTH": 255,
                "CHARACTER_OCTET_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "DATETIME_PRECISION": None,
                "CHARACTER_SET_NAME": None,
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
        self.assertIn("FROM INFORMATION_SCHEMA.COLUMNS", call_args)
        self.assertIn(f"TABLE_NAME = '{table_name}'", call_args)
        self.assertIn("COLUMN_NAME", call_args)
        self.assertIn("DATA_TYPE", call_args)
        self.assertIn("IS_NULLABLE", call_args)
        self.assertNotIn("MYSQL_DATA_TYPE", call_args)

        self.assertEqual(response.type, RESPONSE_TYPE.COLUMNS_TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)

        #  Verify  (including MYSQL_DATA_TYPE added by to_columns_table_response)
        self.assertListEqual(sorted(list(response.data_frame.columns)), sorted(list(INF_SCHEMA_COLUMNS_NAMES_SET)))

        self.assertEqual(response.data_frame.iloc[0]["COLUMN_NAME"], "COL1")
        self.assertEqual(response.data_frame.iloc[0]["DATA_TYPE"], "VARCHAR")
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
        mock_conn.cursor.return_value = mock_cursor
        self.handler.connect = MagicMock(return_value=mock_conn)

        # region test numeric types
        """Test data obtained using:
            CREATE TABLE test_numeric_types (
                n_number NUMBER,
                n_number_p NUMBER(38),
                n_number_ps NUMBER(10,2),
                n_int INTEGER,
                n_integer INTEGER,
                n_bigint BIGINT,
                n_smallint SMALLINT,
                n_tinyint TINYINT,
                n_byteint BYTEINT,
                n_float FLOAT,
                n_float4 FLOAT4,
                n_float8 FLOAT8,
                n_double DOUBLE,
                n_double_precision DOUBLE PRECISION,
                n_real REAL,
                n_decimal DECIMAL(10,2),
                n_numeric NUMERIC(10,2)
            );

            INSERT INTO test_numeric_types (
                n_number,
                n_number_p,
                n_number_ps,
                n_int,
                n_integer,
                n_bigint,
                n_smallint,
                n_tinyint,
                n_byteint,
                n_float,
                n_float4,
                n_float8,
                n_double,
                n_double_precision,
                n_real,
                n_decimal,
                n_numeric
            ) VALUES (
                123456.789,                       -- n_number
                12345678901234567890123456789012345678, -- n_number_p (38 numbers)
                1234.56,                          -- n_number_ps
                2147483647,                       -- n_int
                -2147483648,                      -- n_integer
                9223372036854775807,              -- n_bigint
                32767,                            -- n_smallint
                255,                              -- n_tinyint
                127,                              -- n_byteint
                3.14159265358979,                 -- n_float
                3.14159,                          -- n_float4
                3.141592653589793238,             -- n_float8
                2.7182818284590452,               -- n_double
                1.6180339887498948,               -- n_double_precision
                0.5772156649015329,               -- n_real
                9876.54,                          -- n_decimal
                1234.56                           -- n_numeric
            );
        """
        input_data = pd.DataFrame(
            {
                "N_NUMBER": pd.Series([123457], dtype="int32"),
                "N_NUMBER_P": pd.Series([Decimal("12345678901234567890123456789012345678")], dtype="object"),
                "N_NUMBER_PS": pd.Series([1234.56], dtype="float64"),
                "N_INT": pd.Series([2147483647], dtype="int32"),
                "N_INTEGER": pd.Series([-2147483648], dtype="int32"),
                "N_BIGINT": pd.Series([9223372036854775807], dtype="int64"),
                "N_SMALLINT": pd.Series([32767], dtype="int16"),
                "N_TINYINT": pd.Series([255], dtype="int16"),
                "N_BYTEINT": pd.Series([127], dtype="int8"),
                "N_FLOAT": pd.Series([3.14159265358979], dtype="float64"),
                "N_FLOAT4": pd.Series([3.14159], dtype="float64"),
                "N_FLOAT8": pd.Series([3.141592653589793], dtype="float64"),
                "N_DOUBLE": pd.Series([2.718281828459045], dtype="float64"),
                "N_DOUBLE_PRECISION": pd.Series([1.618033988749895], dtype="float64"),
                "N_REAL": pd.Series([0.5772156649015329], dtype="float64"),
                "N_DECIMAL": pd.Series([9876.54], dtype="float64"),
                "N_NUMERIC": pd.Series([1234.56], dtype="float64"),
            }
        )
        mock_cursor.fetch_pandas_batches.return_value = iter([input_data])
        mock_cursor.description = [
            ColumnDescription(
                name="N_NUMBER",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_NUMBER_P",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_NUMBER_PS",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=10,
                scale=2,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_INT",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_INTEGER",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_BIGINT",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_SMALLINT",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_TINYINT",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_BYTEINT",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=38,
                scale=0,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_FLOAT",
                type_code=1,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_FLOAT4",
                type_code=1,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_FLOAT8",
                type_code=1,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_DOUBLE",
                type_code=1,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_DOUBLE_PRECISION",
                type_code=1,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_REAL",
                type_code=1,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_DECIMAL",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=10,
                scale=2,
                is_nullable=True,
            ),
            ColumnDescription(
                name="N_NUMERIC",
                type_code=0,
                display_size=None,
                internal_size=None,
                precision=10,
                scale=2,
                is_nullable=True,
            ),
        ]

        excepted_mysql_types = [
            MYSQL_DATA_TYPE.MEDIUMINT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.MEDIUMINT,
            MYSQL_DATA_TYPE.MEDIUMINT,
            MYSQL_DATA_TYPE.BIGINT,
            MYSQL_DATA_TYPE.SMALLINT,
            MYSQL_DATA_TYPE.SMALLINT,
            MYSQL_DATA_TYPE.TINYINT,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DOUBLE,
        ]

        response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for column_name in input_data.columns:
            result_value = response.data_frame[column_name][0]
            self.assertEqual(result_value, input_data[column_name][0])
        # endregion

        # region test string/blob types
        """Data obtained using:
            CREATE TABLE test_text_blob_types (
                t_string STRING,                      -- up to 16 МБ
                t_string_size STRING(100),            -- STRING with max len
                t_char CHAR(10),                      -- fix len
                t_varchar VARCHAR(100),               -- STRING alias
                t_text TEXT,                          -- STRING alias
                t_binary BINARY,                      -- bin data up to 8 МБ
                t_binary_size BINARY(100),            -- bin with max len
                t_varbinary VARBINARY(100)            -- BINARY alias
            );

            INSERT INTO test_text_blob_types (
                t_string,
                t_string_size,
                t_char,
                t_varchar,
                t_text,
                t_binary,
                t_binary_size,
                t_varbinary
            ) VALUES (
                't_string',                  -- t_string
                't_string_size',             -- t_string_size
                't_char',                    -- t_char
                't_varchar',                 -- t_varchar
                't_text',                    -- t_text
                TO_BINARY('t_binary', 'UTF-8'),         -- t_binary
                TO_BINARY('t_binary_size', 'UTF-8'),    -- t_binary_size
                TO_BINARY('t_variant', 'UTF-8')         -- t_varbinary
            );
        """
        input_data = pd.DataFrame(
            [
                [
                    "t_string",
                    "t_string_size",
                    "t_char",
                    "t_varchar",
                    "t_text",
                    b"t_binary",
                    b"t_binary_size",
                    b"t_variant",
                ]
            ],
            columns=[
                "T_STRING",
                "T_STRING_SIZE",
                "T_CHAR",
                "T_VARCHAR",
                "T_TEXT",
                "T_BINARY",
                "T_BINARY_SIZE",
                "T_VARBINARY",
            ],
            dtype="object",
        )
        mock_cursor.fetch_pandas_batches.return_value = iter([input_data])
        mock_cursor.description = [
            ColumnDescription(
                name="T_STRING",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_STRING_SIZE",
                type_code=2,
                display_size=None,
                internal_size=100,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_CHAR",
                type_code=2,
                display_size=None,
                internal_size=10,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_VARCHAR",
                type_code=2,
                display_size=None,
                internal_size=100,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_TEXT",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_BINARY",
                type_code=11,
                display_size=None,
                internal_size=8388608,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_BINARY_SIZE",
                type_code=11,
                display_size=None,
                internal_size=100,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_VARBINARY",
                type_code=11,
                display_size=None,
                internal_size=100,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
        ]
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.BINARY,
        ]

        response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for column_name in input_data.columns:
            result_value = response.data_frame[column_name][0]
            self.assertEqual(result_value, input_data[column_name][0])
        # endregion

        # region test bool types
        """Data obtained using:
        CREATE TABLE test_boolean_types (
            b_boolean BOOLEAN
        );

        INSERT INTO test_boolean_types (
            b_boolean
        ) VALUES (
            TRUE                   -- b_boolean
        );
        """
        input_data = pd.DataFrame([[True]], columns=["B_BOOLEAN"], dtype="bool")
        mock_cursor.fetch_pandas_batches.return_value = iter([input_data])
        mock_cursor.description = [
            ColumnDescription(
                name="B_BOOLEAN",
                type_code=13,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            )
        ]
        excepted_mysql_types = [MYSQL_DATA_TYPE.BOOLEAN]

        response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for column_name in input_data.columns:
            result_value = response.data_frame[column_name][0]
            self.assertEqual(result_value, input_data[column_name][0])
        # endregion

        # region test date/time types
        """Data obtained using:
        CREATE TABLE test_datetime_types (
            d_date DATE,
            d_datetime DATETIME,
            d_datetime_p DATETIME(3),
            d_time TIME,
            d_time_p TIME(6),
            d_timestamp TIMESTAMP,
            d_timestamp_p TIMESTAMP(9),
            d_timestamp_ltz TIMESTAMP_LTZ,      -- timestamp with local tz
            d_timestamp_ltz_p TIMESTAMP_LTZ(3),
            d_timestamp_ntz TIMESTAMP_NTZ,      -- timestamp no tz
            d_timestamp_ntz_p TIMESTAMP_NTZ(6),
            d_timestamp_tz TIMESTAMP_TZ,        -- timestamp with tz
            d_timestamp_tz_p TIMESTAMP_TZ(9)
        );

        INSERT INTO test_datetime_types (
            d_date,
            d_datetime,
            d_datetime_p,
            d_time,
            d_time_p,
            d_timestamp,
            d_timestamp_p,
            d_timestamp_ltz,
            d_timestamp_ltz_p,
            d_timestamp_ntz,
            d_timestamp_ntz_p,
            d_timestamp_tz,
            d_timestamp_tz_p
        ) VALUES (
            '2023-10-15',                                -- d_date
            '2023-10-15 14:30:45.123456789',             -- d_datetime
            '2023-10-15 14:30:45.123',                   -- d_datetime_p
            '14:30:45',                                  -- d_time
            '14:30:45.123456',                           -- d_time_p
            '2023-10-15 14:30:45.123456789 +03:00',      -- d_timestamp
            '2023-10-15 14:30:45.123456789 +03:00',      -- d_timestamp_p
            '2023-10-15 14:30:45.123 +03:00',            -- d_timestamp_ltz
            '2023-10-15 14:30:45.123 +03:00',            -- d_timestamp_ltz_p
            '2023-10-15 14:30:45.123456',                -- d_timestamp_ntz
            '2023-10-15 14:30:45.123456',                -- d_timestamp_ntz_p
            '2023-10-15 14:30:45.123456789 +03:00',      -- d_timestamp_tz
            '2023-10-15 14:30:45.123456789 +03:00'       -- d_timestamp_tz_p
        );
        """
        input_data = pd.DataFrame(
            {
                "D_DATE": pd.Series([datetime.date(2023, 10, 15)], dtype="object"),
                "D_DATETIME": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456789")], dtype="datetime64[ns]"),
                "D_DATETIME_P": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123000")], dtype="datetime64[ms]"),
                "D_TIME": pd.Series([datetime.time(14, 30, 45)], dtype="object"),
                "D_TIME_P": pd.Series([datetime.time(14, 30, 45, 123456)], dtype="object"),
                "D_TIMESTAMP": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456789")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_P": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456789")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_LTZ": pd.Series(
                    [pd.Timestamp("2023-10-15 04:30:45.123000-0700", tz="America/Los_Angeles")],
                    dtype="datetime64[ns, America/Los_Angeles]",
                ),
                "D_TIMESTAMP_LTZ_P": pd.Series(
                    [pd.Timestamp("2023-10-15 04:30:45.123000-0700", tz="America/Los_Angeles")],
                    dtype="datetime64[ns, America/Los_Angeles]",
                ),
                "D_TIMESTAMP_NTZ": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_NTZ_P": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_TZ": pd.Series(
                    [pd.Timestamp("2023-10-15 04:30:45.123456789-0700", tz="America/Los_Angeles")],
                    dtype="datetime64[ns, America/Los_Angeles]",
                ),
                "D_TIMESTAMP_TZ_P": pd.Series(
                    [pd.Timestamp("2023-10-15 04:30:45.123456789-0700", tz="America/Los_Angeles")],
                    dtype="datetime64[ns, America/Los_Angeles]",
                ),
            }
        )
        mock_cursor.fetch_pandas_batches.return_value = iter([input_data])
        mock_cursor.description = [
            ColumnDescription(
                name="D_DATE",
                type_code=3,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_DATETIME",
                type_code=8,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_DATETIME_P",
                type_code=8,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=3,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIME",
                type_code=12,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIME_P",
                type_code=12,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=6,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP",
                type_code=8,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP_P",
                type_code=8,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP_LTZ",
                type_code=6,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP_LTZ_P",
                type_code=6,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=3,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP_NTZ",
                type_code=8,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP_NTZ_P",
                type_code=8,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=6,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP_TZ",
                type_code=7,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
            ColumnDescription(
                name="D_TIMESTAMP_TZ_P",
                type_code=7,
                display_size=None,
                internal_size=None,
                precision=0,
                scale=9,
                is_nullable=True,
            ),
        ]
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.DATE,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.TIME,
            MYSQL_DATA_TYPE.TIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.DATETIME,
        ]
        expected_result_df = pd.DataFrame(
            {
                "D_DATE": pd.Series([datetime.date(2023, 10, 15)], dtype="object"),
                "D_DATETIME": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456789")], dtype="datetime64[ns]"),
                "D_DATETIME_P": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123000")], dtype="datetime64[ms]"),
                "D_TIME": pd.Series([datetime.time(14, 30, 45)], dtype="object"),
                "D_TIME_P": pd.Series([datetime.time(14, 30, 45, 123456)], dtype="object"),
                "D_TIMESTAMP": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456789")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_P": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456789")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_LTZ": pd.Series([pd.Timestamp("2023-10-15 11:30:45.123000")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_LTZ_P": pd.Series([pd.Timestamp("2023-10-15 11:30:45.123000")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_NTZ": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_NTZ_P": pd.Series([pd.Timestamp("2023-10-15 14:30:45.123456")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_TZ": pd.Series([pd.Timestamp("2023-10-15 11:30:45.123456789")], dtype="datetime64[ns]"),
                "D_TIMESTAMP_TZ_P": pd.Series([pd.Timestamp("2023-10-15 11:30:45.123456789")], dtype="datetime64[ns]"),
            }
        )
        response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        self.assertTrue(response.data_frame.equals(expected_result_df))
        # endregion

        # region json/array types
        """Test request:

        select * from demo_snowflake (
            select
                OBJECT_CONSTRUCT('name', 'Jones', 'age', 42) as t_json,
                ARRAY_CONSTRUCT(12, 'twelve', NULL) as t_array,
                [1.1,2.2,3.3]::VECTOR(FLOAT,3) as t_vector
        );
        """
        input_data = pd.DataFrame(
            {
                "T_JSON": pd.Series([{"name": "Jones", "age": 42}], dtype="object"),
                # snowflake returns arrays as text
                "T_ARRAY": pd.Series(['[\n  12,\n  "twelve",\n  undefined\n]'], dtype="object"),
                "T_VECTOR": pd.Series([np.array([1.1, 2.2, 3.3], dtype="float32")], dtype="object"),
            }
        )
        mock_cursor.fetch_pandas_batches.return_value = iter([input_data])
        mock_cursor.description = [
            ColumnDescription(
                name="T_JSON",
                type_code=9,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_ARRAY",
                type_code=10,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
            ColumnDescription(
                name="T_VECTOR",
                type_code=16,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                is_nullable=True,
            ),
        ]

        excepted_mysql_types = [MYSQL_DATA_TYPE.JSON, MYSQL_DATA_TYPE.JSON, MYSQL_DATA_TYPE.VECTOR]

        expected_result_df = pd.DataFrame(
            {
                "T_JSON": pd.Series([{"name": "Jones", "age": 42}], dtype="object"),
                "T_ARRAY": pd.Series(['[\n  12,\n  "twelve",\n  undefined\n]'], dtype="object"),
                "T_VECTOR": pd.Series([np.array([1.1, 2.2, 3.3], dtype="float32")], dtype="object"),
            }
        )
        response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        self.assertTrue(response.data_frame.equals(expected_result_df))
        # endregion


if __name__ == "__main__":
    unittest.main()
