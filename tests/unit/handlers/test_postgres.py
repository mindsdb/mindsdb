import unittest
import datetime
from uuid import UUID
from decimal import Decimal
from zoneinfo import ZoneInfo
from collections import OrderedDict
from unittest.mock import patch, MagicMock

import psycopg
from psycopg.pq import ExecStatus
from psycopg.postgres import types as pg_types
import numpy as np
import pandas as pd
from pandas import DataFrame
from pandas.api import types as pd_types

from base_handler_test import BaseDatabaseHandlerTest, MockCursorContextManager
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response, RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


class ColumnDescription:
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.type_code = kwargs.get("type_code")
        self.type_display = kwargs.get("type_display")


# map between regtype name and type id
regtype_to_oid = {t.regtype: t.oid for t in pg_types}
type_name_to_oid = {t.name: t.oid for t in pg_types}
type_name_to_array_oid = {t.name: t.array_oid for t in pg_types}


class TestPostgresHandler(BaseDatabaseHandlerTest, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host="127.0.0.1",
            port=5432,
            user="example_user",
            schema="public",
            password="example_pass",
            database="example_db",
            sslmode="prefer",
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
                table_name = '{self.mock_table}'
            AND
                table_schema = current_schema()
        """

    def create_handler(self):
        return PostgresHandler("psql", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("psycopg.connect")

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
        mock_cursor.rowcount = 1

        query_str = "INSERT INTO table VALUES (1, 2, 3)"
        data = self.handler.native_query(query_str)
        mock_cursor.execute.assert_called_once_with(query_str)
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)
        self.assertEqual(data.type, RESPONSE_TYPE.OK)
        self.assertEqual(data.affected_rows, 1)

    def test_native_query_with_results(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and handles the case
        where the query returns a result set
        """
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager()

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_cursor.fetchall = MagicMock(return_value=[[1, "name1"], [2, "name2"]])

        # Create proper description objects with necessary type_code for _cast_dtypes
        mock_cursor.description = [
            ColumnDescription(name="id", type_code=regtype_to_oid["integer"]),  # int4 type code
            ColumnDescription(name="name", type_code=regtype_to_oid["text"]),  # text type code
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
        self.assertEqual(list(data.data_frame.columns), ["id", "name"])

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
        params = [(1, "a"), (2, "b")]
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
        df = pd.DataFrame(
            {
                "int2_col": ["1", "2"],
                "int4_col": ["10", "20"],
                "int8_col": ["100", "200"],
                "numeric_col": ["1.5", "2.5"],
                "float4_col": ["1.1", "2.2"],
                "float8_col": ["10.1", "20.2"],
                "text_col": ["a", "b"],
            }
        )

        original_get = psycopg.postgres.types.get

        try:
            type_mocks = {}
            for pg_type, oid in type_name_to_oid.items():
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
                    default_mock.name = "unknown"
                    return default_mock

            psycopg.postgres.types.get = mock_get

            description = [
                ColumnDescription(name="int2_col", type_code=type_name_to_oid["int2"]),
                ColumnDescription(name="int4_col", type_code=type_name_to_oid["int4"]),
                ColumnDescription(name="int8_col", type_code=type_name_to_oid["int8"]),
                ColumnDescription(name="numeric_col", type_code=type_name_to_oid["numeric"]),
                ColumnDescription(name="float4_col", type_code=type_name_to_oid["float4"]),
                ColumnDescription(name="float8_col", type_code=type_name_to_oid["float8"]),
                ColumnDescription(name="text_col", type_code=type_name_to_oid["text"]),
            ]

            self.handler._cast_dtypes(df, description)
            # Verify the types were correctly cast
            self.assertEqual(df["int2_col"].dtype, "int16")
            self.assertEqual(df["int4_col"].dtype, "int32")
            self.assertEqual(df["int8_col"].dtype, "int64")
            self.assertEqual(df["numeric_col"].dtype, "float64")
            self.assertEqual(df["float4_col"].dtype, "float32")
            self.assertEqual(df["float8_col"].dtype, "float64")
            self.assertEqual(df["text_col"].dtype, "object")

        finally:
            # Restore original function
            psycopg.postgres.types.get = original_get

    def test_cast_dtypes_with_nulls(self):
        """
        Tests the _cast_dtypes method with NULL values to ensure correct handling
        """
        df = pd.DataFrame({"int2_col": ["1", None], "float4_col": ["1.1", None]})

        # Create type code mapping
        type_codes = {
            "int2": 21,  # Typical OID for int2
            "float4": 700,  # Typical OID for float4
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
                    default_mock.name = "unknown"
                    return default_mock

            psycopg.postgres.types.get = mock_get

            # Set up description with our custom class
            description = [
                ColumnDescription(name="int2_col", type_code=type_codes["int2"]),
                ColumnDescription(name="float4_col", type_code=type_codes["float4"]),
            ]

            self.handler._cast_dtypes(df, description)

            self.assertEqual(df["int2_col"].dtype, "int16")
            self.assertEqual(df["float4_col"].dtype, "float32")
            self.assertEqual(df["int2_col"].iloc[1], 0)
            self.assertEqual(df["float4_col"].iloc[1], 0)

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

        mock_pgresult = MagicMock()
        mock_pgresult.status = ExecStatus.TUPLES_OK
        mock_cursor.pgresult = mock_pgresult
        mock_cursor.rowcount = 1
        mock_cursor.fetchall = MagicMock(
            return_value=[
                ["a", "int", 1, None, "YES", None, None, None, None, None, None, None],
                ["b", "int", 2, None, "YES", None, None, None, None, None, None, None],
                ["c", "int", 3, None, "YES", None, None, None, None, None, None, None],
            ]
        )
        information_schema_description = [
            ColumnDescription(name="COLUMN_NAME", type_code=regtype_to_oid["text"]),
            ColumnDescription(name="DATA_TYPE", type_code=regtype_to_oid["text"]),
            ColumnDescription(name="ORDINAL_POSITION", type_code=regtype_to_oid["integer"]),
            ColumnDescription(name="COLUMN_DEFAULT", type_code=regtype_to_oid["text"]),
            ColumnDescription(name="IS_NULLABLE", type_code=regtype_to_oid["text"]),
            ColumnDescription(name="CHARACTER_MAXIMUM_LENGTH", type_code=regtype_to_oid["integer"]),
            ColumnDescription(name="CHARACTER_OCTET_LENGTH", type_code=regtype_to_oid["integer"]),
            ColumnDescription(name="NUMERIC_PRECISION", type_code=regtype_to_oid["integer"]),
            ColumnDescription(name="NUMERIC_SCALE", type_code=regtype_to_oid["integer"]),
            ColumnDescription(name="DATETIME_PRECISION", type_code=regtype_to_oid["integer"]),
            ColumnDescription(name="CHARACTER_SET_NAME", type_code=regtype_to_oid["text"]),
            ColumnDescription(name="COLLATION_NAME", type_code=regtype_to_oid["text"]),
        ]
        mock_cursor.description = information_schema_description

        # Create mock for copy operation
        copy_obj = MagicMock()
        mock_cursor.copy = MagicMock(return_value=copy_obj)
        # Ensure copy.__enter__ returns the copy object to mimic context manager
        copy_obj.__enter__ = MagicMock(return_value=copy_obj)
        copy_obj.__exit__ = MagicMock(return_value=None)

        # region add result for 'get_columns' call
        mock_pgresult = MagicMock()
        mock_pgresult.status = ExecStatus.TUPLES_OK
        mock_cursor.pgresult = mock_pgresult
        mock_cursor.fetchall = MagicMock(
            return_value=[
                ["id", "int", 1, None, "YES", None, None, None, None, None, None, None],
                ["name", "text", 2, None, "YES", None, None, None, None, None, None, None],
            ]
        )
        mock_cursor.description = information_schema_description
        # endregino

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        self.handler.insert("test_table", df)

        # Verify copy was called with correct SQL
        copy_sql = 'copy "test_table" ("id","name") from STDIN WITH CSV'
        mock_cursor.copy.assert_called_once_with(copy_sql)
        # commit for get_columns and insert
        self.assertEqual(mock_conn.commit.call_count, 2)

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
        # Before calling copy, get_columns is called
        mock_cursor.execute = MagicMock(side_effect=error)
        mock_cursor.copy = MagicMock(side_effect=error)

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        # Call the insert method and expect an exception
        with self.assertRaisesRegex(ValueError, "Table doesn't exist"):
            self.handler.insert("nonexistent_table", df)

        mock_conn.rollback.assert_called()

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
        self.handler.connection_args["connection_parameters"] = {"application_name": "mindsdb_test", "keepalives": 1}

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]

        self.assertEqual(call_kwargs["application_name"], "mindsdb_test")
        self.assertEqual(call_kwargs["keepalives"], 1)
        self.assertEqual(call_kwargs["connect_timeout"], 10)
        self.assertEqual(call_kwargs["sslmode"], "prefer")

        expected_options = "-c search_path=public,public"
        self.assertEqual(call_kwargs["options"], expected_options)

        # Test with a different schema
        # Create a fresh handler with different schema
        self.tearDown()
        self.setUp()
        self.handler.connection_args["schema"] = "custom_schema"
        self.handler.connection_args["connection_parameters"] = {"application_name": "mindsdb_test"}

        self.handler.connect()
        call_kwargs = self.mock_connect.call_args[1]
        expected_options = "-c search_path=custom_schema,public"
        self.assertEqual(call_kwargs["options"], expected_options)

    def test_types_casting(self):
        """Test that types are casted correctly"""
        query_str = "SELECT * FROM test_table"

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        mock_pgresult = MagicMock()
        mock_pgresult.status = ExecStatus.TUPLES_OK
        mock_cursor.pgresult = mock_pgresult
        # mock_conn.is_connected = MagicMock(return_value=True)

        # region test TEXT/BLOB types and sub-types
        """Test data obtained from:

            CREATE TABLE test_text_blob_types (
                id SERIAL PRIMARY KEY,
                t_char CHAR(10),
                t_varchar VARCHAR(100),
                t_text TEXT,
                t_bytea BYTEA,
                t_json JSON,
                t_jsonb JSONB,
                t_xml XML,
                t_uuid UUID
            );

            INSERT INTO test_text_blob_types (
                t_char, t_varchar, t_text, t_bytea, t_json, t_jsonb, t_xml, t_uuid
            ) VALUES (
                'Test',
                'Test',
                'Test',
                E'\\x44656D6F2062696E61727920646174612E',
                '{"name": "test"}',
                '{"name": "test"}',
                '<root><element>test</element><nested><value>123</value></nested></root>',
                'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
            );
        """
        input_row = (
            "Test      ",
            "Test",
            "Test",
            b"Demo binary data.",
            {"name": "test"},
            {"name": "test"},
            "<root><element>test</element><nested><value>123</value></nested></root>",
            UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        )
        mock_cursor.fetchall.return_value = [input_row]

        description = [
            ColumnDescription(name="t_char", type_code=type_name_to_oid["bpchar"]),
            ColumnDescription(name="t_varchar", type_code=type_name_to_oid["varchar"]),
            ColumnDescription(name="t_text", type_code=type_name_to_oid["text"]),
            ColumnDescription(name="t_bytea", type_code=type_name_to_oid["bytea"]),
            ColumnDescription(name="t_json", type_code=type_name_to_oid["json"]),
            ColumnDescription(name="t_jsonb", type_code=type_name_to_oid["jsonb"]),
            ColumnDescription(name="t_xml", type_code=type_name_to_oid["xml"]),
            ColumnDescription(name="t_uuid", type_code=type_name_to_oid["uuid"]),
        ]
        mock_cursor.description = description
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.VARCHAR,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.BINARY,
            MYSQL_DATA_TYPE.JSON,
            MYSQL_DATA_TYPE.JSON,
            MYSQL_DATA_TYPE.VARCHAR,
            MYSQL_DATA_TYPE.VARCHAR,
        ]
        response: Response = self.handler.native_query(query_str)

        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[description[i].name][0]
            self.assertEqual(type(result_value), type(input_value), f"type mismatch: {result_value} != {input_value}")
            self.assertEqual(result_value, input_value, f"value mismatch: {result_value} != {input_value}")
        # endregion

        # region test BOOLEAN type
        input_rows = [(True,), (False,)]
        mock_cursor.fetchall.return_value = input_rows
        mock_cursor.description = [ColumnDescription(name="t_boolean", type_code=16)]
        excepted_mysql_types = [MYSQL_DATA_TYPE.BOOL]
        response: Response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        self.assertTrue(pd_types.is_bool_dtype(response.data_frame["t_boolean"][0]))
        self.assertTrue(bool(response.data_frame["t_boolean"][0]) is True)
        self.assertTrue(bool(response.data_frame["t_boolean"][1]) is False)
        # endregion

        # region test numeric types
        """Test data obtained from:

        CREATE TABLE test_numeric_types (
            n_smallint SMALLINT,
            n_integer INTEGER,
            n_bigint BIGINT,
            n_decimal DECIMAL(10,2),
            n_numeric NUMERIC(10,4),
            n_real REAL,
            n_double_precision DOUBLE PRECISION,
            n_smallserial SMALLSERIAL,
            n_serial SERIAL,
            n_bigserial BIGSERIAL,
            n_money MONEY,
            n_int2 INT2,        -- alt for SMALLINT
            n_int4 INT4,        -- alt for INTEGER
            n_int8 INT8,        -- alt for BIGINT
            n_float4 FLOAT4,    -- alt for REAL
            n_float8 FLOAT8     -- alt for DOUBLE PRECISION
        );

        INSERT INTO test_numeric_types (
            n_smallint,
            n_integer,
            n_bigint,
            n_decimal,
            n_numeric,
            n_real,
            n_double_precision,
            n_money,
            n_int2,
            n_int4,
            n_int8,
            n_float4,
            n_float8
        ) VALUES (
            32767,                  -- n_smallint (max value)
            2147483647,             -- n_integer (max value)
            9223372036854775807,    -- n_bigint (max value)
            1234.56,                -- n_decimal
            12345.6789,             -- n_numeric
            3.14159,                -- n_real
            2.7182818284590452,     -- n_double_precision
            '$10,500.25',           -- n_money
            -32768,                 -- n_int2 (min value)
            42,                     -- n_int4
            123456789,              -- n_int8
            0.00123,                -- n_float4
            9.8765432109876         -- n_float8
        );
        """
        input_row = (
            32767,  # n_smallint (max value)
            2147483647,  # n_integer (max value)
            9223372036854775807,  # n_bigint (max value)
            Decimal("1234.56"),  # n_decimal
            Decimal("12345.6789"),  # n_numeric
            3.14159,  # n_real
            2.718281828459045,  # n_double_precision
            1,  # n_smallserial
            1,  # n_serial
            1,  # n_bigserial
            "$10,500.25",  # n_money
            -32768,  # n_int2
            42,  # n_int4
            123456789,  # n_int8
            0.00123,  # n_float4
            9.8765432109876,  # n_float8
        )
        mock_cursor.fetchall.return_value = [input_row]

        description = [
            ColumnDescription(name="n_smallint", type_code=21),
            ColumnDescription(name="n_integer", type_code=23),
            ColumnDescription(name="n_bigint", type_code=20),
            ColumnDescription(name="n_decimal", type_code=1700),
            ColumnDescription(name="n_numeric", type_code=1700),
            ColumnDescription(name="n_real", type_code=700),
            ColumnDescription(name="n_double_precision", type_code=701),
            ColumnDescription(name="n_smallserial", type_code=21),
            ColumnDescription(name="n_serial", type_code=23),
            ColumnDescription(name="n_bigserial", type_code=20),
            ColumnDescription(name="n_money", type_code=790),
            ColumnDescription(name="n_int2", type_code=21),
            ColumnDescription(name="n_int4", type_code=23),
            ColumnDescription(name="n_int8", type_code=20),
            ColumnDescription(name="n_float4", type_code=700),
            ColumnDescription(name="n_float8", type_code=701),
        ]
        mock_cursor.description = description

        excepted_mysql_types = [
            MYSQL_DATA_TYPE.SMALLINT,  # n_smallint
            MYSQL_DATA_TYPE.INT,  # n_integer
            MYSQL_DATA_TYPE.BIGINT,  # n_bigint
            MYSQL_DATA_TYPE.DECIMAL,  # n_decimal
            MYSQL_DATA_TYPE.DECIMAL,  # n_numeric
            MYSQL_DATA_TYPE.FLOAT,  # n_real
            MYSQL_DATA_TYPE.DOUBLE,  # n_double_precision
            MYSQL_DATA_TYPE.SMALLINT,  # n_smallserial
            MYSQL_DATA_TYPE.INT,  # n_serial
            MYSQL_DATA_TYPE.BIGINT,  # n_bigserial
            MYSQL_DATA_TYPE.TEXT,  # n_money
            MYSQL_DATA_TYPE.SMALLINT,  # n_int2
            MYSQL_DATA_TYPE.INT,  # n_int4
            MYSQL_DATA_TYPE.BIGINT,  # n_int8
            MYSQL_DATA_TYPE.FLOAT,  # n_float4
            MYSQL_DATA_TYPE.DOUBLE,  # n_float8
        ]
        response: Response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[description[i].name][0]
            self.assertEqual(result_value, input_value, f"value mismatch: {result_value} != {input_value}")
        # endregion

        # region test datetime types
        """Test data obtained from:

            CREATE TABLE test_time_types (
                t_date DATE,
                t_time TIME,
                t_time_tz TIME WITH TIME ZONE,
                t_timestamp TIMESTAMP,
                t_timestamp_tz TIMESTAMP WITH TIME ZONE,
                t_interval INTERVAL,
                t_timestamptz TIMESTAMPTZ,
                t_timetz TIMETZ
            );

            INSERT INTO test_time_types (
                t_date,
                t_time,
                t_time_tz,
                t_timestamp,
                t_timestamp_tz,
                t_interval,
                t_timestamptz,
                t_timetz
            ) VALUES (
                '2023-10-15',                                -- t_date
                '14:30:45',                                  -- t_time
                '14:30:45+03:00',                            -- t_time_tz
                '2023-10-15 14:30:45',                       -- t_timestamp
                '2023-10-15 14:30:45+03:00',                 -- t_timestamp_tz
                '2 years 3 months 15 days 12 hours 30 minutes 15 seconds', -- t_interval
                '2023-10-15 14:30:45+03:00',                 -- t_timestamptz
                '14:30:45+03:00'                             -- t_timetz
            );
        """
        input_row = (
            datetime.date(2023, 10, 15),
            datetime.time(14, 30, 45),
            datetime.time(14, 30, 45, tzinfo=datetime.timezone(datetime.timedelta(seconds=10800))),
            datetime.datetime(2023, 10, 15, 14, 30, 45),
            datetime.datetime(2023, 10, 15, 11, 30, 45, tzinfo=ZoneInfo(key="Etc/UTC")),
            datetime.timedelta(days=835, seconds=45015),
            datetime.datetime(2023, 10, 15, 11, 30, 45, tzinfo=ZoneInfo(key="Etc/UTC")),
            datetime.time(14, 30, 45, tzinfo=datetime.timezone(datetime.timedelta(seconds=10800))),
        )
        mock_cursor.fetchall.return_value = [input_row]

        description = [
            ColumnDescription(name="t_date", type_code=1082),
            ColumnDescription(name="t_time", type_code=1083),
            ColumnDescription(name="t_time_tz", type_code=1266),
            ColumnDescription(name="t_timestamp", type_code=1114),
            ColumnDescription(name="t_timestamp_tz", type_code=1184),
            ColumnDescription(name="t_interval", type_code=1186),
            ColumnDescription(name="t_timestamptz", type_code=1184),
            ColumnDescription(name="t_timetz", type_code=1266),
        ]
        mock_cursor.description = description

        excepted_mysql_types = [
            MYSQL_DATA_TYPE.DATE,  # DATE
            MYSQL_DATA_TYPE.TIME,  # TIME
            MYSQL_DATA_TYPE.TIME,  # TIME WITH TIME ZONE
            MYSQL_DATA_TYPE.DATETIME,  # TIMESTAMP
            MYSQL_DATA_TYPE.DATETIME,  # TIMESTAMP WITH TIME ZONE
            MYSQL_DATA_TYPE.VARCHAR,  # INTERVAL
            MYSQL_DATA_TYPE.DATETIME,  # TIMESTAMPTZ
            MYSQL_DATA_TYPE.TIME,  # TIMETZ
        ]

        response: Response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[description[i].name][0]
            self.assertEqual(result_value, input_value, f"value mismatch: {result_value} != {input_value}")
        # endregion

        # region test casting of nullable types
        bigint_val = 9223372036854775807
        input_rows = [(bigint_val, True), (None, None)]
        mock_cursor.fetchall.return_value = input_rows
        description = [
            ColumnDescription(name="n_bigint", type_code=20),
            ColumnDescription(name="t_boolean", type_code=16),
        ]
        mock_cursor.description = description
        response: Response = self.handler.native_query(query_str)
        self.assertEqual(response.data_frame.dtypes[0], "Int64")
        self.assertEqual(response.data_frame.dtypes[1], "boolean")
        self.assertEqual(response.data_frame.iloc[0, 0], bigint_val)
        self.assertEqual(response.data_frame.iloc[0, 1], True)
        self.assertTrue(response.data_frame.iloc[1, 0] is pd.NA)
        self.assertTrue(response.data_frame.iloc[1, 1] is pd.NA)
        # endregion

        # region test arrays and vector
        """Note: for vector type need to install pgvector extension
           Test data obtained from:

            CREATE TABLE test_array_types (
                int_arr1 integer[],
                int_arr2 integer[][],
                text_arr1 text[],
                embedding vector(3)
            );

            INSERT INTO test_array_types (
                int_arr1,
                int_arr2,
                text_arr1,
                embedding
            ) VALUES (
                '{1,null,3}',
                '{{1,2,3},{4,null,6}}',
                '{"test1", null, "test3"}',
                '[1.1, 2.2, 3.3]'
            );
        """
        input_row = (
            [1, None, 3],  # int_arr1
            [[1, 2, 3], [4, None, 6]],  # int_arr2
            ["test1", None, "test3"],  # text_arr1
            np.array([1.1, 2.2, 3.3], dtype="float32"),
        )
        mock_cursor.fetchall.return_value = [input_row]

        description = [
            ColumnDescription(name="int_arr1", type_code=type_name_to_array_oid["int4"]),
            ColumnDescription(name="int_arr2", type_code=type_name_to_array_oid["int4"]),
            ColumnDescription(name="text_arr1", type_code=type_name_to_array_oid["varchar"]),
            ColumnDescription(name="embedding", type_code=16390, type_display="vector"),
        ]
        mock_cursor.description = description

        excepted_mysql_types = [
            MYSQL_DATA_TYPE.JSON,
            MYSQL_DATA_TYPE.JSON,
            MYSQL_DATA_TYPE.JSON,
            MYSQL_DATA_TYPE.VECTOR,
        ]

        response: Response = self.handler.native_query(query_str)
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for i, input_value in enumerate(input_row):
            result_value = response.data_frame[description[i].name][0]
            self.assertEqual(type(result_value), type(input_value), f"type mismatch: {result_value} != {input_value}")
            if isinstance(result_value, list):
                self.assertEqual(result_value, input_value, f"value mismatch: {result_value} != {input_value}")
            else:
                self.assertTrue(np.all(result_value == input_value))
        # endregion


if __name__ == "__main__":
    unittest.main()
