import unittest
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict
from typing import List

import pytest
import pandas as pd

try:
    from databricks.sql import RequestError, ServerOperationError
    from mindsdb.integrations.handlers.databricks_handler.databricks_handler import (
        DatabricksHandler,
    )

    DATABRICKS_AVAILABLE = True
except ImportError:
    pytestmark = pytest.mark.skip("Databricks handler not installed")
    DATABRICKS_AVAILABLE = False

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE,
    HandlerStatusResponse as StatusResponse,
)


class CursorContextManager:
    """Mock cursor that supports context manager protocol."""

    def __init__(self):
        self.description = []
        self._results = []
        self.execute = MagicMock()
        self.fetchall = MagicMock(return_value=[])
        self.fetchone = MagicMock(return_value=None)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def set_results(self, results: List[tuple], columns: List[str]):
        """Set mock query results."""
        self._results = results
        self.description = [(col,) for col in columns]
        self.fetchall = MagicMock(return_value=results)
        self.fetchone = MagicMock(return_value=results[0] if results else None)


# The correct patch path - patch where 'connect' is looked up, not where it's defined
CONNECT_PATCH_PATH = (
    "mindsdb.integrations.handlers.databricks_handler.databricks_handler.connect"
)


@pytest.mark.skipif(not DATABRICKS_AVAILABLE, reason="Databricks not installed")
class TestInstallationCheck(unittest.TestCase):
    """Test handler installation and information schema."""

    def test_handler_import(self):
        """Verify handler is properly installed and can be imported."""
        from mindsdb.integrations.handlers.databricks_handler import databricks_handler

        self.assertTrue(hasattr(databricks_handler, "DatabricksHandler"))

    def test_handler_name(self):
        """Verify handler has correct name attribute."""
        self.assertEqual(DatabricksHandler.name, "databricks")

    def test_connection_args_validation(self):
        """Verify required connection args are validated."""
        # Missing required params should raise ValueError
        handler = DatabricksHandler("test", connection_data={})
        with self.assertRaises(ValueError) as ctx:
            handler.connect()
        self.assertIn("server_hostname", str(ctx.exception))
        self.assertIn("http_path", str(ctx.exception))
        self.assertIn("access_token", str(ctx.exception))


@pytest.mark.skipif(not DATABRICKS_AVAILABLE, reason="Databricks not installed")
class TestDatabricksHandler(unittest.TestCase):
    dummy_connection_data = OrderedDict(
        server_hostname="adb-1234567890123456.7.azuredatabricks.net",
        http_path="sql/protocolv1/o/1234567890123456/1234-567890-test123",
        access_token="dapi1234567890ab1cde2f3ab456c7d89efa",
    )

    def setUp(self):
        # Patch where connect is used, not where it's defined
        self.patcher = patch(CONNECT_PATCH_PATH)
        self.mock_connect = self.patcher.start()
        self.handler = DatabricksHandler(
            "databricks", connection_data=self.dummy_connection_data
        )

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        """
        Tests if the `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        """
        mock_conn = MagicMock()
        self.mock_connect.return_value = mock_conn

        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Tests if the `connect` method correctly handles a connection failure by raising a databricks.sql.RequestError and sets is_connected to False.
        """
        self.mock_connect.side_effect = RequestError("Connection Failed")

        with self.assertRaises(RequestError):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection_success(self):
        """
        Tests if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        mock_conn = MagicMock()
        mock_cursor = CursorContextManager()
        mock_cursor.set_results([(1,)], ["1"])
        mock_conn.cursor.return_value = mock_cursor
        self.mock_connect.return_value = mock_conn

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        self.assertIsInstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_check_connection_failure(self):
        """
        Tests if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        self.mock_connect.side_effect = RequestError("Connection Failed")

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertIsInstance(response, StatusResponse)
        self.assertTrue(response.error_message)


@pytest.mark.skipif(not DATABRICKS_AVAILABLE, reason="Databricks not installed")
class TestTableOperations(unittest.TestCase):
    """Test table operations (DDL & DML)."""

    dummy_connection_data = OrderedDict(
        server_hostname="test.azuredatabricks.net",
        http_path="sql/test",
        access_token="test_token",
        schema="default",
    )

    def setUp(self):
        # Patch where connect is used, not where it's defined
        self.patcher = patch(CONNECT_PATCH_PATH)
        self.mock_connect = self.patcher.start()
        self.mock_conn = MagicMock()
        self.mock_cursor = CursorContextManager()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_connect.return_value = self.mock_conn
        self.handler = DatabricksHandler(
            "databricks", connection_data=self.dummy_connection_data
        )

    def tearDown(self):
        self.patcher.stop()

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor and returns a Response object.
        """
        self.mock_cursor.set_results([], [])

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)

        self.mock_cursor.execute.assert_called_once_with(query_str)
        self.assertIsInstance(data, Response)
        self.assertFalse(data.error_code)

    def test_get_tables(self):
        """
        Tests if the `get_tables` method to confirm it correctly calls `native_query` with the appropriate SQL commands.
        """
        self.handler.native_query = MagicMock()

        self.handler.get_tables()

        expected_query = """
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema != 'information_schema'
                and table_schema = current_schema()
        """
        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_columns(self):
        """
        Tests if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()

        table_name = "mock_table"
        self.handler.get_columns(table_name)

        expected_query = f"""
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
                null as CHARACTER_SET_NAME,
                null as COLLATION_NAME
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
            AND
                table_schema = current_schema()
        """

        self.handler.native_query.assert_called_once_with(expected_query)

    def test_native_query_server_error(self):
        """Test query execution with server error."""
        self.mock_cursor.execute = MagicMock(
            side_effect=ServerOperationError("Server error")
        )

        result = self.handler.native_query("SELECT * FROM test_table")

        self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Server error", result.error_message)

    def test_get_tables_all_schemas(self):
        """Test get_tables with all=True."""
        self.handler.native_query = MagicMock(
            return_value=Response(
                RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame([{"table_name": "t1"}])
            )
        )

        self.handler.get_tables(all=True)

        query = self.handler.native_query.call_args[0][0]
        self.assertNotIn("table_schema = current_schema()", query)

    def test_get_columns_with_schema(self):
        """Test get_columns with explicit schema."""
        mock_df = pd.DataFrame(
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "INT",
                    "ORDINAL_POSITION": 1,
                    "COLUMN_DEFAULT": None,
                    "IS_NULLABLE": "NO",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "CHARACTER_OCTET_LENGTH": None,
                    "NUMERIC_PRECISION": 10,
                    "NUMERIC_SCALE": 0,
                    "DATETIME_PRECISION": None,
                    "CHARACTER_SET_NAME": None,
                    "COLLATION_NAME": None,
                }
            ]
        )

        self.handler.native_query = MagicMock(
            return_value=Response(RESPONSE_TYPE.TABLE, data_frame=mock_df)
        )

        self.handler.get_columns("test_table", schema_name="my_schema")

        query = self.handler.native_query.call_args[0][0]
        self.assertIn("'my_schema'", query)


if __name__ == "__main__":
    unittest.main()
