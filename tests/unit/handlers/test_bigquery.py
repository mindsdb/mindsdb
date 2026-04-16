import unittest
import pytest
import pandas as pd
from collections import OrderedDict
from unittest.mock import patch, MagicMock
from google.api_core.exceptions import BadRequest, NotFound

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
    TableResponse,
    ErrorResponse,
)

try:
    from mindsdb.integrations.handlers.bigquery_handler.bigquery_handler import BigQueryHandler
except ImportError:
    pytestmark = pytest.mark.skip("Bigquery handler not installed")


class TestBigQueryHandler(unittest.TestCase):
    dummy_connection_data = OrderedDict(
        project_id="tough-future-332513",
        dataset="example_ds",
        service_account_keys="example_keys",
    )

    def setUp(self):
        self.patcher_get_oauth2_credentials = patch(
            "mindsdb.integrations.utilities.handlers.auth_utilities.google.GoogleServiceAccountOAuth2Manager.get_oauth2_credentials"
        )
        self.patcher_client = patch("mindsdb.integrations.handlers.bigquery_handler.bigquery_handler.Client")
        self.mock_get_oauth2_credentials = self.patcher_get_oauth2_credentials.start()
        self.mock_connect = self.patcher_client.start()
        self.handler = BigQueryHandler("bigquery", connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher_get_oauth2_credentials.stop()
        self.patcher_client.stop()

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that google.cloud.bigquery.Client is called exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """
        Ensures that the connect method correctly handles a connection failure by raising a google.api_core.exceptions.BadRequest and sets is_connected to False.
        """
        self.mock_connect.side_effect = BadRequest("Connection Failed")

        with self.assertRaises(BadRequest):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection(self):
        """
        Verifies that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        connected = self.handler.check_connection()
        self.assertTrue(connected)
        assert isinstance(connected, StatusResponse)
        self.assertFalse(connected.error_message)

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query using the mock query object and returns a Response object.
        """
        mock_conn = MagicMock()
        self.handler.connect = MagicMock(return_value=mock_conn)

        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = pd.DataFrame({"col": [1, 2, 3]})
        mock_conn.query.return_value = mock_query

        query_str = "SELECT * FROM table"

        with patch(
            "mindsdb.integrations.handlers.bigquery_handler.bigquery_handler.QueryJobConfig"
        ) as mock_query_job_config:
            mock_query_job_config_instance = mock_query_job_config.return_value
            data = self.handler.native_query(query_str)
            mock_conn.query.assert_called_once_with(query_str, job_config=mock_query_job_config_instance)
            assert isinstance(data, TableResponse)

    def test_native_query_empty_select_returns_table(self):
        mock_conn = MagicMock()
        self.handler.connect = MagicMock(return_value=mock_conn)

        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = pd.DataFrame(columns=["id"])
        mock_conn.query.return_value = mock_query

        with patch("mindsdb.integrations.handlers.bigquery_handler.bigquery_handler.QueryJobConfig"):
            response = self.handler.native_query("SELECT id FROM table WHERE 1 = 0")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(list(response.data_frame.columns), ["id"])
        self.assertTrue(response.data_frame.empty)

    def test_native_query_empty_dataframe_without_columns_returns_ok(self):
        mock_conn = MagicMock()
        self.handler.connect = MagicMock(return_value=mock_conn)

        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = pd.DataFrame()
        mock_conn.query.return_value = mock_query

        with patch("mindsdb.integrations.handlers.bigquery_handler.bigquery_handler.QueryJobConfig"):
            response = self.handler.native_query("UPDATE table SET col = 1")

        self.assertEqual(response.type, RESPONSE_TYPE.OK)

    def test_get_tables(self):
        """
        Checks if the `get_tables` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()

        self.handler.get_tables()

        expected_query = f"""
            SELECT table_name, table_schema, table_type
            FROM `{self.dummy_connection_data["project_id"]}.{self.dummy_connection_data["dataset"]}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type IN ('BASE TABLE', 'VIEW')
        """

        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_columns(self):
        """
        Checks if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        self.handler.native_query = MagicMock()

        table_name = "mock_table"
        self.handler.get_columns(table_name)

        expected_query = f"""
            SELECT column_name AS Field, data_type as Type
            FROM `{self.dummy_connection_data["project_id"]}.{self.dummy_connection_data["dataset"]}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """

        self.handler.native_query.assert_called_once_with(expected_query)

    def test_meta_get_tables_filters(self):
        self.handler.native_query = MagicMock(return_value=TableResponse(data=pd.DataFrame()))

        self.handler.meta_get_tables(table_names=["orders"])

        query = self.handler.native_query.call_args[0][0]
        self.assertIn("AND t.table_name IN ('orders')", query)

    def test_meta_get_columns_filters(self):
        self.handler.native_query = MagicMock(return_value=TableResponse(data=pd.DataFrame()))

        self.handler.meta_get_columns(table_names=["orders"])

        query = self.handler.native_query.call_args[0][0]
        self.assertIn("WHERE table_name IN ('orders')", query)

    def test_meta_get_column_statistics_batches_results(self):
        columns = [f"col_{i}" for i in range(22)]

        # First response: column types query (required by meta_get_column_statistics_for_table)
        column_types_result = pd.DataFrame(
            {
                "column_name": columns,
                "data_type": ["INT64"] * 22,  # All columns are INT64 type
            }
        )

        # Second response: first batch statistics (20 columns)
        first_batch_result = pd.DataFrame(
            {
                "table_name": ["table"] * 20,
                "column_name": [f"col_{i}" for i in range(20)],
                "null_percentage": [0.0] * 20,
                "minimum_value": ["1"] * 20,
                "maximum_value": ["10"] * 20,
                "distinct_values_count": [10] * 20,
            }
        )

        # Third response: second batch statistics (2 columns)
        second_batch_result = pd.DataFrame(
            {
                "table_name": ["table"] * 2,
                "column_name": ["col_20", "col_21"],
                "null_percentage": [0.0, 50.0],
                "minimum_value": ["1", "a"],
                "maximum_value": ["10", "z"],
                "distinct_values_count": [10, 20],
            }
        )

        self.handler.native_query = MagicMock(
            side_effect=[
                TableResponse(data=column_types_result),
                TableResponse(data=first_batch_result),
                TableResponse(data=second_batch_result),
            ]
        )

        response = self.handler.meta_get_column_statistics_for_table("table", columns)

        self.assertEqual(response.resp_type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(response.data_frame), 22)  # Total of 20 + 2 = 22 columns
        self.assertEqual(self.handler.native_query.call_count, 3)  # 1 for column types + 2 for batches

    def test_meta_get_column_statistics_returns_error_when_empty(self):
        self.handler.native_query = MagicMock(return_value=ErrorResponse(error_message="boom"))

        response = self.handler.meta_get_column_statistics_for_table("table", ["col"])
        self.assertEqual(response.resp_type, RESPONSE_TYPE.ERROR)

    def test_meta_get_primary_keys_filters(self):
        self.handler.native_query = MagicMock(return_value=TableResponse(data=pd.DataFrame()))
        self.handler.meta_get_primary_keys(table_names=["orders"])

        query = self.handler.native_query.call_args[0][0]
        self.assertIn("AND tc.table_name IN ('orders')", query)
        self.assertNotIn("tc.constraint_name,", query)

    def test_meta_get_foreign_keys_filters(self):
        self.handler.native_query = MagicMock(return_value=TableResponse(data=pd.DataFrame()))
        self.handler.meta_get_foreign_keys(table_names=["orders"])
        query = self.handler.native_query.call_args[0][0]
        self.assertIn("AND tc.table_name IN ('orders')", query)

    def test_connect_uses_billing_project(self):
        """Test that billing_project overrides project_id when passed to Client."""
        handler = BigQueryHandler(
            "bigquery",
            connection_data=OrderedDict(
                project_id="tough-future-332513",
                dataset="example_ds",
                service_account_keys="example_keys",
                billing_project="billing-project-123",
            ),
        )
        self.mock_connect.return_value = MagicMock()
        handler.connect()
        self.mock_connect.assert_called_once_with(
            project="billing-project-123",
            credentials=self.mock_get_oauth2_credentials.return_value,
        )

    def test_check_connection_dataset_not_found(self):
        """Test that NotFound from get_dataset returns a failed StatusResponse."""
        mock_conn = MagicMock()
        mock_conn.get_dataset.side_effect = NotFound("dataset not found")
        self.mock_connect.return_value = mock_conn

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertIn("not found", str(response.error_message))

    def test_check_connection_resets_is_connected_on_failure(self):
        """Test that is_connected is reset to False when a connected handler fails check."""
        mock_conn = MagicMock()
        mock_conn.query.side_effect = BadRequest("query failed")
        self.handler.connection = mock_conn
        self.handler.is_connected = True

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertFalse(self.handler.is_connected)

    def test_disconnect(self):
        """Test that disconnect closes the connection and sets is_connected to False."""
        self.mock_connect.return_value = MagicMock()
        self.handler.connect()
        self.assertTrue(self.handler.is_connected)
        conn = self.handler.connection

        self.handler.disconnect()

        self.assertFalse(self.handler.is_connected)
        conn.close.assert_called_once()

    def test_query(self):
        """Test that query renders AST to SQL and delegates to native_query."""
        from mindsdb_sql_parser import parse_sql

        self.handler.native_query = MagicMock()
        ast = parse_sql("SELECT 1")
        self.handler.query(ast)

        self.handler.native_query.assert_called_once()
        call_arg = self.handler.native_query.call_args[0][0]
        self.assertIsInstance(call_arg, str)
        self.assertTrue(len(call_arg) > 0)

    def test_meta_get_column_statistics_skips_minmax_for_unsupported_types(self):
        """Test that ARRAY/JSON columns use NULL placeholders instead of MIN/MAX."""
        columns = ["arr_col", "json_col", "int_col"]
        column_types_result = pd.DataFrame(
            {
                "column_name": columns,
                "data_type": ["ARRAY<INT64>", "JSON", "INT64"],
            }
        )
        stats_result = pd.DataFrame(
            {
                "table_name": ["table"] * 3,
                "column_name": columns,
                "null_percentage": [0.0] * 3,
                "minimum_value": [None, None, "1"],
                "maximum_value": [None, None, "10"],
                "distinct_values_count": [None, None, 5],
            }
        )
        self.handler.native_query = MagicMock(
            side_effect=[
                TableResponse(data=column_types_result),
                TableResponse(data=stats_result),
            ]
        )

        response = self.handler.meta_get_column_statistics_for_table("table", columns)

        self.assertEqual(response.resp_type, RESPONSE_TYPE.TABLE)
        stats_query = self.handler.native_query.call_args_list[1][0][0]
        self.assertIn("CAST(NULL AS STRING)", stats_query)

    def test_get_tables_uses_dataset_project(self):
        """Test that dataset_project overrides project_id in SQL when provided."""
        handler = BigQueryHandler(
            "bigquery",
            connection_data=OrderedDict(
                project_id="tough-future-332513",
                dataset="example_ds",
                service_account_keys="example_keys",
                dataset_project="other-project",
            ),
        )
        handler.native_query = MagicMock()
        handler.get_tables()

        query = handler.native_query.call_args[0][0]
        self.assertIn("other-project", query)
        self.assertNotIn("tough-future-332513", query)


if __name__ == "__main__":
    unittest.main()
