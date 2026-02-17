import unittest
from unittest.mock import patch, MagicMock
from collections import OrderedDict
from typing import List

import pytest
import pandas as pd

try:
    from databricks.sql import RequestError, ServerOperationError
    from mindsdb_sql_parser import parse_sql
    from mindsdb.integrations.handlers.verified.databricks_handler.databricks_handler import (
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


CONNECT_PATCH_PATH = "mindsdb.integrations.handlers.verified.databricks_handler.databricks_handler.connect"


@pytest.mark.skipif(not DATABRICKS_AVAILABLE, reason="Databricks not installed")
class TestInstallationCheck(unittest.TestCase):
    """Test handler installation and information schema."""

    def test_handler_import(self):
        """Verify handler is properly installed and can be imported."""
        from mindsdb.integrations.handlers.verified.databricks_handler import databricks_handler

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
        self.handler = DatabricksHandler("databricks", connection_data=self.dummy_connection_data)

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
        self.patcher = patch(CONNECT_PATCH_PATH)
        self.mock_connect = self.patcher.start()
        self.mock_conn = MagicMock()
        self.mock_cursor = CursorContextManager()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_connect.return_value = self.mock_conn
        self.handler = DatabricksHandler("databricks", connection_data=self.dummy_connection_data)

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
        self.mock_cursor.execute = MagicMock(side_effect=ServerOperationError("Server error"))

        result = self.handler.native_query("SELECT * FROM test_table")

        self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Server error", result.error_message)

    def test_get_tables_all_schemas(self):
        """Test get_tables with all=True."""
        self.handler.native_query = MagicMock(
            return_value=Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame([{"table_name": "t1"}]))
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

        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.TABLE, data_frame=mock_df))

        self.handler.get_columns("test_table", schema_name="my_schema")

        query = self.handler.native_query.call_args[0][0]
        self.assertIn("'my_schema'", query)


@pytest.mark.skipif(not DATABRICKS_AVAILABLE, reason="Databricks not installed")
class TestAdvancedQueries(unittest.TestCase):
    dummy_connection_data = OrderedDict(
        server_hostname="test.azuredatabricks.net",
        http_path="sql/test",
        access_token="test_token",
        schema="default",
    )

    def setUp(self):
        self.patcher = patch(CONNECT_PATCH_PATH)
        self.mock_connect = self.patcher.start()
        self.mock_conn = MagicMock()
        self.mock_cursor = CursorContextManager()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_connect.return_value = self.mock_conn
        self.handler = DatabricksHandler("databricks", connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_aggregation_count(self):
        """Test COUNT(*) aggregation."""
        self.mock_cursor.set_results([(100,)], ["count"])

        result = self.handler.native_query("SELECT COUNT(*) as count FROM orders")

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(result.data_frame.iloc[0]["count"], 100)

    def test_aggregation_sum_group_by(self):
        """Test SUM with GROUP BY."""
        self.mock_cursor.set_results(
            [("Electronics", 50000.0), ("Clothing", 25000.0)],
            ["category", "total_sales"],
        )

        result = self.handler.native_query("SELECT category, SUM(amount) as total_sales FROM sales GROUP BY category")

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 2)

    def test_cte_query(self):
        """Test SELECT with CTE."""
        self.mock_cursor.set_results([(1, "Product A", 100)], ["id", "name", "quantity"])

        query = """
        WITH cte AS (
            SELECT * FROM products WHERE quantity > 50
        )
        SELECT * FROM cte WHERE id = 1
        """

        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.mock_cursor.execute.assert_called()

    def test_join_query(self):
        """Test JOIN query."""
        self.mock_cursor.set_results([(1, "Order 1", "Customer A")], ["order_id", "order_name", "customer_name"])

        query = """
        SELECT o.id as order_id, o.name as order_name, c.name as customer_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        """

        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)


@pytest.mark.skipif(not DATABRICKS_AVAILABLE, reason="Databricks not installed")
class TestDateTimeFunctions(unittest.TestCase):
    """Test date/time functions and INTERVAL transformations."""

    dummy_connection_data = OrderedDict(
        server_hostname="test.azuredatabricks.net",
        http_path="sql/test",
        access_token="test_token",
        schema="default",
    )

    def setUp(self):
        self.patcher = patch(CONNECT_PATCH_PATH)
        self.mock_connect = self.patcher.start()
        self.mock_conn = MagicMock()
        self.mock_cursor = CursorContextManager()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_connect.return_value = self.mock_conn
        self.handler = DatabricksHandler("databricks", connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_current_timestamp(self):
        """Test CURRENT_TIMESTAMP function."""
        from datetime import datetime

        now = datetime.now()
        self.mock_cursor.set_results([(now,)], ["current_timestamp"])

        result = self.handler.native_query("SELECT CURRENT_TIMESTAMP as current_timestamp")

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(result.data_frame.iloc[0]["current_timestamp"], now)

    def test_date_add_interval_days_native_query(self):
        """Test DATE_ADD with INTERVAL days via native_query."""
        from datetime import datetime, timedelta

        base_date = datetime(2023, 1, 1)
        expected_date = base_date + timedelta(days=30)
        self.mock_cursor.set_results([(expected_date,)], ["due_date"])

        query = """
            SELECT DATE_ADD(o_orderdate, 30) as due_date
            FROM orders
            LIMIT 5
        """

        result = self.handler.native_query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(result.data_frame.iloc[0]["due_date"], expected_date)

    def test_query_transforms_date_add_day_interval(self):
        """Test DATE_ADD with INTERVAL DAY is transformed to integer argument."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '30' DAY) AS due_date FROM orders LIMIT 1")
        # breakpoint()
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("date_add(o_orderdate, 30)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_days_plural(self):
        """Test DATE_ADD with INTERVAL DAYS (plural) is transformed correctly."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL 7 DAYS) AS due_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("date_add(o_orderdate, 7)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_day_interval(self):
        """Test DATE_SUB with INTERVAL DAY is transformed to integer argument."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '5' DAY) AS past_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("date_sub(o_orderdate, 5)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_week_interval(self):
        """Test DATE_ADD with INTERVAL WEEK is converted to days."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '2' WEEK) AS future_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("date_add(o_orderdate, 14)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_week_interval(self):
        """Test DATE_SUB with INTERVAL WEEK is converted to days."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '2' WEEK) AS past_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("date_sub(o_orderdate, 14)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_month_interval(self):
        """Test DATE_ADD with INTERVAL MONTH uses ADD_MONTHS function."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '2' MONTH) AS future_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("add_months(o_orderdate, 2)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_month_interval(self):
        """Test DATE_SUB with INTERVAL MONTH uses ADD_MONTHS with negative value."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '3' MONTH) AS past_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("add_months(o_orderdate, -3)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_year_interval(self):
        """Test DATE_ADD with INTERVAL YEAR uses ADD_MONTHS with 12x multiplier."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '1' YEAR) AS future_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("add_months(o_orderdate, 12)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_year_interval(self):
        """Test DATE_SUB with INTERVAL YEAR uses ADD_MONTHS with negative 12x value."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '2' YEAR) AS past_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("add_months(o_orderdate, -24)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_hour_interval(self):
        """Test DATE_ADD with INTERVAL HOUR uses TIMESTAMPADD function."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '6' HOUR) AS future_time FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("timestampadd(hour, 6, o_orderdate)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_hour_interval(self):
        """Test DATE_SUB with INTERVAL HOUR uses TIMESTAMPADD with negative value."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '3' HOUR) AS past_time FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("timestampadd(hour, -3, o_orderdate)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_minute_interval(self):
        """Test DATE_ADD with INTERVAL MINUTE uses TIMESTAMPADD function."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '30' MINUTE) AS future_time FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("timestampadd(minute, 30, o_orderdate)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_second_interval(self):
        """Test DATE_ADD with INTERVAL SECOND uses TIMESTAMPADD function."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '45' SECOND) AS future_time FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("timestampadd(second, 45, o_orderdate)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_without_interval_unchanged(self):
        """Test that queries without INTERVAL pass through unchanged."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, 10) AS future_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("date_add(o_orderdate, 10)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_add_quarter_interval(self):
        """Test DATE_ADD with INTERVAL QUARTER uses ADD_MONTHS with 3x multiplier."""
        query = parse_sql("SELECT DATE_ADD(o_orderdate, INTERVAL '2' QUARTER) AS future_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("add_months(o_orderdate, 6)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_quarter_interval(self):
        """Test DATE_SUB with INTERVAL QUARTER uses ADD_MONTHS with negative 3x value."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '1' QUARTER) AS past_date FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("add_months(o_orderdate, -3)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_minute_interval(self):
        """Test DATE_SUB with INTERVAL MINUTE uses TIMESTAMPADD with negative value."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '15' MINUTE) AS past_time FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("timestampadd(minute, -15, o_orderdate)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)

    def test_query_transforms_date_sub_second_interval(self):
        """Test DATE_SUB with INTERVAL SECOND uses TIMESTAMPADD with negative value."""
        query = parse_sql("SELECT DATE_SUB(o_orderdate, INTERVAL '30' SECOND) AS past_time FROM orders")
        self.handler.native_query = MagicMock(return_value=Response(RESPONSE_TYPE.OK))

        self.handler.query(query)

        transformed_sql = self.handler.native_query.call_args[0][0].lower()
        self.assertIn("timestampadd(second, -30, o_orderdate)", transformed_sql)
        self.assertNotIn("interval", transformed_sql)


if __name__ == "__main__":
    unittest.main()
