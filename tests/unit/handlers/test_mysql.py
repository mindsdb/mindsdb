import unittest
import datetime
from array import array
from decimal import Decimal
from collections import OrderedDict
from unittest.mock import patch, MagicMock

import mysql.connector
import pandas as pd
from pandas import DataFrame
from pandas.api import types as pd_types

from base_handler_test import BaseDatabaseHandlerTest, MockCursorContextManager
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response, INF_SCHEMA_COLUMNS_NAMES_SET, RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


class TestMySQLHandler(BaseDatabaseHandlerTest, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="password",
            database="test_db",
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
        return f"""
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
                table_name = '{self.mock_table}';
        """

    def create_handler(self):
        return MySQLHandler("mysql", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("mysql.connector.connect")

    def test_native_query(self):
        """Test that native_query returns a Response object with no error"""
        mock_conn = MagicMock()
        mock_cursor = MockCursorContextManager(
            data=[{"id": 1}], description=[("id", 3, None, None, None, None, 1, 0, 45)]
        )

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)

        query_str = f"SELECT * FROM {self.mock_table}"
        data = self.handler.native_query(query_str)

        self.assertIsInstance(data, Response)
        self.assertFalse(data.error_code)

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

        mock_cursor.fetchall.return_value = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

        # MySQL cursor provides column info via description attribute
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
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

        expected_columns = ["id", "name"]
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

    def test_check_connection_success(self):
        """
        Tests that check_connection returns success status when connection is valid
        """
        self.handler.connection_data = self.dummy_connection_data.copy()

        mock_conn = MagicMock()
        mock_conn.is_connected = MagicMock(return_value=True)
        self.mock_connect.return_value = mock_conn

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)
        self.mock_connect.assert_called_once()

    def test_check_connection_failure(self):
        """
        Tests that check_connection returns failure status and error message when connection fails
        """
        self.handler.connection_data = self.dummy_connection_data.copy()

        error_message = "Connection failed: Unknown MySQL server host 'invalid-host'"
        self.mock_connect.side_effect = mysql.connector.Error(error_message)

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertIsNotNone(response.error_message)
        self.assertIn("Connection failed", response.error_message)

    def test_check_connection_closes_on_success(self):
        """
        Tests that check_connection closes the connection after successful check if it wasn't already connected
        """
        self.handler.connection_data = self.dummy_connection_data.copy()
        self.handler.connection = None  # Not connected initially

        mock_conn = MagicMock()
        mock_conn.is_connected = MagicMock(return_value=True)
        self.mock_connect.return_value = mock_conn

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        mock_conn.close.assert_called_once()

    def test_connection_with_url(self):
        """
        Tests connecting with a URL connection string instead of individual parameters
        """
        url_connection_data = {"url": "mysql://root:password@127.0.0.1:3306/test_db"}
        self.handler.connection_data = url_connection_data

        # Mock ConnectionConfig to process the URL
        with patch("mindsdb.integrations.handlers.mysql_handler.mysql_handler.ConnectionConfig") as mock_config_class:
            mock_model = MagicMock()
            mock_model.model_dump.return_value = {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "password",
                "database": "test_db",
                "connection_timeout": 10,
                "collation": "utf8mb4_general_ci",
                "use_pure": True,
            }
            mock_config_class.return_value = mock_model

            self.handler.connect()

            mock_config_class.assert_called_once_with(**url_connection_data)
            self.mock_connect.assert_called_once()

    def test_unpack_config(self):
        """
        Tests the _unpack_config method to ensure it correctly validates and unpacks connection data
        """
        with patch("mindsdb.integrations.handlers.mysql_handler.mysql_handler.ConnectionConfig") as mock_config_class:
            mock_model = MagicMock()
            mock_model.model_dump.return_value = {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "password",
                "database": "test_db",
            }
            mock_config_class.return_value = mock_model

            valid_config = self.dummy_connection_data.copy()
            self.handler.connection_data = valid_config

            config = self.handler._unpack_config()
            mock_config_class.assert_called_once_with(**valid_config)
            mock_model.model_dump.assert_called_once_with(exclude_unset=True)

            self.assertEqual(config["host"], "127.0.0.1")
            self.assertEqual(config["port"], 3306)
            self.assertEqual(config["user"], "root")
            self.assertEqual(config["password"], "password")
            self.assertEqual(config["database"], "test_db")

            mock_config_class.side_effect = ValueError("Invalid config")
            with self.assertRaises(ValueError):
                self.handler._unpack_config()

    def test_connect_with_ssl(self):
        """
        Tests connecting with SSL configuration to ensure SSL parameters are correctly passed
        """
        self.handler.connection_data = self.dummy_connection_data.copy()
        self.handler.connection_data["ssl"] = True
        self.handler.connection_data["ssl_ca"] = "/path/to/ca.pem"
        self.handler.connection_data["ssl_cert"] = "/path/to/cert.pem"
        self.handler.connection_data["ssl_key"] = "/path/to/key.pem"

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertIn("client_flags", call_kwargs)
        self.assertIn(mysql.connector.constants.ClientFlag.SSL, call_kwargs["client_flags"])
        self.assertEqual(call_kwargs["ssl_ca"], "/path/to/ca.pem")
        self.assertEqual(call_kwargs["ssl_cert"], "/path/to/cert.pem")
        self.assertEqual(call_kwargs["ssl_key"], "/path/to/key.pem")

    def test_connect_sets_configuration(self):
        """
        Tests that connect method correctly sets default configuration values when not provided
        """
        self.handler.connection_data = {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "password": "password",
            "database": "test_db",
        }

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertEqual(call_kwargs["connection_timeout"], 10)
        self.assertEqual(call_kwargs["collation"], "utf8mb4_general_ci")
        self.assertEqual(call_kwargs["use_pure"], True)

        # Verify autocommit was set on the connection
        self.mock_connect.return_value.autocommit = True

    def test_query_method(self):
        """
        Tests the query method to ensure it correctly converts ASTNode to SQL and calls native_query
        """
        with patch("mindsdb.integrations.handlers.mysql_handler.mysql_handler.SqlalchemyRender") as mock_renderer_class:
            mock_renderer = MagicMock()
            mock_renderer.get_string.return_value = "SELECT * FROM test"
            mock_renderer_class.return_value = mock_renderer

            self.handler.native_query = MagicMock()
            self.handler.native_query.return_value = Response(RESPONSE_TYPE.OK)

            mock_ast = MagicMock()

            result = self.handler.query(mock_ast)

            mock_renderer_class.assert_called_once_with("mysql")

            mock_renderer.get_string.assert_called_once_with(mock_ast, with_failback=True)

            self.handler.native_query.assert_called_once_with("SELECT * FROM test")
            self.assertEqual(result, self.handler.native_query.return_value)

    def test_connection_with_conn_attrs(self):
        """
        Tests connecting with connection attributes to ensure they are correctly passed
        """
        self.handler.connection_data = self.dummy_connection_data.copy()
        self.handler.connection_data["conn_attrs"] = {"program_name": "mindsdb", "client_version": "1.0"}

        self.handler.connect()

        call_kwargs = self.mock_connect.call_args[1]
        self.assertEqual(call_kwargs["conn_attrs"], {"program_name": "mindsdb", "client_version": "1.0"})

    def test_get_tables(self):
        """
        Tests that get_tables calls native_query with the correct SQL
        """
        expected_response = Response(RESPONSE_TYPE.OK)
        self.handler.native_query = MagicMock(return_value=expected_response)

        response = self.handler.get_tables()

        self.handler.native_query.assert_called_once()
        call_args = self.handler.native_query.call_args[0][0]

        self.assertIn("information_schema.TABLES", call_args)
        self.assertIn("TABLE_SCHEMA", call_args)
        self.assertIn("TABLE_NAME", call_args)
        self.assertIn("TABLE_TYPE", call_args)

        self.assertEqual(response, expected_response)

    def test_get_columns(self):
        """
        Tests that get_columns calls native_query with the correct SQL
        """
        expected_response = Response(
            RESPONSE_TYPE.TABLE, data_frame=DataFrame([], columns=list(INF_SCHEMA_COLUMNS_NAMES_SET))
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

    def test_types_casting(self):
        """Test that types are casted correctly"""
        query_str = "SELECT * FROM test_table"

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=None)

        self.handler.connect = MagicMock(return_value=mock_conn)
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.is_connected = MagicMock(return_value=True)

        # region test TEXT/BLOB types and sub-types
        input_row = {
            "t_varchar": "v_varchar",
            "t_tinytext": "v_tinytext",
            "t_text": "v_text",
            "t_mediumtext": "v_mediumtext",
            "t_longtext": "v_longtext",
            "t_tinyblon": "v_tinyblon",
            "t_blob": "v_blob",
            "t_mediumblob": "v_mediumblob",
            "t_longblob": "v_longblob",
            "t_json": '{"key": "value"}',
        }
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ("t_varchar", 253, None, None, None, None, 1, 0, 45),
            ("t_tinytext", 252, None, None, None, None, 1, 16, 45),
            ("t_text", 252, None, None, None, None, 1, 16, 45),
            ("t_mediumtext", 252, None, None, None, None, 1, 16, 45),
            ("t_longtext", 252, None, None, None, None, 1, 16, 45),
            ("t_tinyblon", 252, None, None, None, None, 1, 144, 63),
            ("t_blob", 252, None, None, None, None, 1, 144, 63),
            ("t_mediumblob", 252, None, None, None, None, 1, 144, 63),
            ("t_longblob", 252, None, None, None, None, 1, 144, 63),
            ("t_json", 245, None, None, None, None, 1, 144, 63),
        ]

        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.VARBINARY,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.TEXT,
            MYSQL_DATA_TYPE.BLOB,
            MYSQL_DATA_TYPE.BLOB,
            MYSQL_DATA_TYPE.BLOB,
            MYSQL_DATA_TYPE.BLOB,
            MYSQL_DATA_TYPE.JSON,
        ]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for key, input_value in input_row.items():
            result_value = response.data_frame[key][0]
            self.assertEqual(type(result_value), type(input_value))
            self.assertEqual(result_value, input_value)
        # endregion

        # region test TINYINT/BOOL/BOOLEAN types
        input_row = {"t_tinyint": 1, "t_bool": 1, "t_boolean": 1}
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ("t_tinyint", 1, None, None, None, None, 1, 0, 63),
            ("t_bool", 1, None, None, None, None, 1, 0, 63),
            ("t_boolean", 1, None, None, None, None, 1, 0, 63),
        ]
        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [MYSQL_DATA_TYPE.TINYINT, MYSQL_DATA_TYPE.TINYINT, MYSQL_DATA_TYPE.TINYINT]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for key, input_value in input_row.items():
            result_value = response.data_frame[key][0]
            # without None values in result columns types will be one of pandas types
            self.assertTrue(pd_types.is_integer_dtype(result_value))
            self.assertEqual(result_value, input_value)
        # endregion

        # region test numeric types
        input_row = {
            "t_tinyint": 1,
            "t_bool": 0,
            "t_smallint": 2,
            "t_year": 2025,
            "t_mediumint": 3,
            "t_int": 4,
            "t_bigint": 5,
            "t_float": 1.1,
            "t_double": 2.2,
            "t_decimal": Decimal("3.3"),
        }
        mock_cursor.fetchall.return_value = [input_row]
        mock_cursor.description = [
            ("t_tinyint", 1, None, None, None, None, 1, 0, 63),
            ("t_bool", 1, None, None, None, None, 1, 0, 63),
            ("t_smallint", 2, None, None, None, None, 1, 0, 63),
            ("t_year", 13, None, None, None, None, 1, 96, 63),
            ("t_mediumint", 9, None, None, None, None, 1, 0, 63),
            ("t_int", 3, None, None, None, None, 1, 0, 63),
            ("t_bigint", 8, None, None, None, None, 1, 0, 63),
            ("t_float", 4, None, None, None, None, 1, 0, 63),
            ("t_double", 5, None, None, None, None, 1, 0, 63),
            ("t_decimal", 246, None, None, None, None, 1, 0, 63),
        ]
        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.TINYINT,
            MYSQL_DATA_TYPE.TINYINT,
            MYSQL_DATA_TYPE.SMALLINT,
            MYSQL_DATA_TYPE.YEAR,
            MYSQL_DATA_TYPE.MEDIUMINT,
            MYSQL_DATA_TYPE.INT,
            MYSQL_DATA_TYPE.BIGINT,
            MYSQL_DATA_TYPE.FLOAT,
            MYSQL_DATA_TYPE.DOUBLE,
            MYSQL_DATA_TYPE.DECIMAL,
        ]

        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for key, input_value in input_row.items():
            result_value = response.data_frame[key][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # test date/time types
        input_row = {
            "t_date": datetime.date(2025, 4, 16),
            "t_time": datetime.timedelta(seconds=45600),
            "t_year": 2025,
            "t_datetime": datetime.datetime(2025, 4, 16, 12, 30, 15),
            "t_timestamp": datetime.datetime(2025, 4, 16, 12, 30, 15),
        }
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [
            ("t_date", 10, None, None, None, None, 1, 128, 63),
            ("t_time", 11, None, None, None, None, 1, 128, 63),
            ("t_year", 13, None, None, None, None, 1, 96, 63),
            ("t_datetime", 12, None, None, None, None, 1, 128, 63),
            ("t_timestamp", 7, None, None, None, None, 1, 128, 63),
        ]

        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [
            MYSQL_DATA_TYPE.DATE,
            MYSQL_DATA_TYPE.TIME,
            MYSQL_DATA_TYPE.YEAR,
            MYSQL_DATA_TYPE.DATETIME,
            MYSQL_DATA_TYPE.TIMESTAMP,
        ]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        for key, input_value in input_row.items():
            result_value = response.data_frame[key][0]
            self.assertEqual(result_value, input_value)
        # endregion

        # region test casting of nullable types
        bigint_val = 9223372036854775807
        input_rows = [{"t_bigint": bigint_val, "t_boolean": 1}, {"t_bigint": None, "t_boolean": None}]
        mock_cursor.fetchall.return_value = input_rows
        description = [
            ("t_bigint", 8, None, None, None, None, 1, 0, 63),
            ("t_boolean", 1, None, None, None, None, 1, 0, 63),
        ]
        mock_cursor.description = description
        response: Response = self.handler.native_query(query_str)
        self.assertEqual(response.data_frame.dtypes.iloc[0], "Int64")
        self.assertEqual(response.data_frame.dtypes.iloc[1], "Int64")
        self.assertEqual(response.data_frame.iloc[0, 0], bigint_val)
        self.assertEqual(response.data_frame.iloc[0, 1], 1)
        self.assertTrue(response.data_frame.iloc[1, 0] is pd.NA)
        self.assertTrue(response.data_frame.iloc[1, 1] is pd.NA)
        # endregion

        # region test vector type
        input_row = {
            "t_vector": array("f", [1.1, 2.2, 3.3]),
        }
        mock_cursor.fetchall.return_value = [input_row]

        mock_cursor.description = [("t_vector", 242, None, None, None, None, 1, 144, 63)]

        response: Response = self.handler.native_query(query_str)
        excepted_mysql_types = [MYSQL_DATA_TYPE.VECTOR]
        self.assertEqual(response.mysql_types, excepted_mysql_types)
        self.assertEqual(input_row["t_vector"], response.data_frame["t_vector"][0])
        # endregion

    def _test_meta_method_with_filter(self, method, sample_data, filter_column, filter_values):
        """
        Helper method to test meta catalog methods with and without filtering.

        Args:
            method: The method to test (e.g., self.handler.meta_get_tables)
            sample_data: List of dicts containing sample data
            filter_column: Column name to filter on
            filter_values: List of values to filter by
        """
        # Test without filter
        df = DataFrame(sample_data)
        expected_response = Response(RESPONSE_TYPE.TABLE, data_frame=df)
        self.handler.native_query = MagicMock(return_value=expected_response)

        response = method()
        self.handler.native_query.assert_called_once()
        self.assertIs(response, expected_response)

        # Test with filter
        self.handler.native_query.reset_mock()
        filtered_df = df[df[filter_column].isin(filter_values)].reset_index(drop=True)
        filtered_response = Response(RESPONSE_TYPE.TABLE, data_frame=filtered_df)
        self.handler.native_query = MagicMock(return_value=filtered_response)

        response = method(table_names=filter_values)
        self.handler.native_query.assert_called_once()
        self.assertIs(response, filtered_response)

        # Verify filtered data
        if filter_column in response.data_frame.columns:
            self.assertTrue(response.data_frame[filter_column].isin(filter_values).all())

        return response

    def test_meta_get_tables_returns_response(self):
        """Test that meta_get_tables returns correct response with and without filtering"""
        sample_data = [
            {
                "table_name": "customers",
                "table_schema": "test_db",
                "table_type": "BASE TABLE",
                "table_description": "Customer information",
                "row_count": 100,
            },
            {
                "table_name": "orders",
                "table_schema": "test_db",
                "table_type": "BASE TABLE",
                "table_description": None,
                "row_count": 500,
            },
            {
                "table_name": "products",
                "table_schema": "test_db",
                "table_type": "BASE TABLE",
                "table_description": None,
                "row_count": 42,
            },
        ]
        self._test_meta_method_with_filter(
            self.handler.meta_get_tables, sample_data, "table_name", ["customers", "orders"]
        )

    def test_meta_get_columns_returns_response(self):
        """Test that meta_get_columns returns correct response with and without filtering"""
        sample_data = [
            {
                "table_name": "customers",
                "column_name": "id",
                "data_type": "int",
                "column_description": None,
                "column_default": None,
                "is_nullable": 0,
            },
            {
                "table_name": "customers",
                "column_name": "name",
                "data_type": "varchar",
                "column_description": None,
                "column_default": None,
                "is_nullable": 1,
            },
            {
                "table_name": "products",
                "column_name": "sku",
                "data_type": "varchar",
                "column_description": "Product SKU",
                "column_default": None,
                "is_nullable": 0,
            },
        ]
        self._test_meta_method_with_filter(self.handler.meta_get_columns, sample_data, "table_name", ["customers"])

    def test_meta_get_column_statistics_returns_response(self):
        """Test that meta_get_column_statistics returns correct response with and without filtering"""
        sample_data = [
            {
                "TABLE_NAME": "customers",
                "COLUMN_NAME": "id",
                "MOST_COMMON_VALUES": None,
                "MOST_COMMON_FREQUENCIES": None,
                "NULL_PERCENTAGE": 0.0,
                "MINIMUM_VALUE": "1",
                "MAXIMUM_VALUE": "100",
                "DISTINCT_VALUES_COUNT": 100,
            },
            {
                "TABLE_NAME": "customers",
                "COLUMN_NAME": "name",
                "MOST_COMMON_VALUES": None,
                "MOST_COMMON_FREQUENCIES": None,
                "NULL_PERCENTAGE": 5.0,
                "MINIMUM_VALUE": "Alice",
                "MAXIMUM_VALUE": "Zoe",
                "DISTINCT_VALUES_COUNT": 95,
            },
            {
                "TABLE_NAME": "products",
                "COLUMN_NAME": "sku",
                "MOST_COMMON_VALUES": None,
                "MOST_COMMON_FREQUENCIES": None,
                "NULL_PERCENTAGE": 0.0,
                "MINIMUM_VALUE": None,
                "MAXIMUM_VALUE": None,
                "DISTINCT_VALUES_COUNT": 42,
            },
        ]
        self._test_meta_method_with_filter(
            self.handler.meta_get_column_statistics, sample_data, "TABLE_NAME", ["customers"]
        )

    def test_meta_get_primary_keys_returns_response(self):
        """Test that meta_get_primary_keys returns correct response with and without filtering"""
        sample_data = [
            {"table_name": "customers", "column_name": "id", "ordinal_position": 1, "constraint_name": "PRIMARY"},
            {"table_name": "orders", "column_name": "id", "ordinal_position": 1, "constraint_name": "PRIMARY"},
            {"table_name": "products", "column_name": "id", "ordinal_position": 1, "constraint_name": "PRIMARY"},
        ]
        self._test_meta_method_with_filter(self.handler.meta_get_primary_keys, sample_data, "table_name", ["customers"])

    def test_meta_get_foreign_keys_returns_response(self):
        """Test that meta_get_foreign_keys returns correct response with and without filtering"""
        sample_data = [
            {
                "parent_table_name": "customers",
                "parent_column_name": "id",
                "child_table_name": "orders",
                "child_column_name": "customer_id",
                "constraint_name": "orders_ibfk_1",
            },
            {
                "parent_table_name": "products",
                "parent_column_name": "id",
                "child_table_name": "orders",
                "child_column_name": "product_id",
                "constraint_name": "orders_ibfk_2",
            },
        ]
        self._test_meta_method_with_filter(
            self.handler.meta_get_foreign_keys, sample_data, "child_table_name", ["orders"]
        )


if __name__ == "__main__":
    unittest.main()
