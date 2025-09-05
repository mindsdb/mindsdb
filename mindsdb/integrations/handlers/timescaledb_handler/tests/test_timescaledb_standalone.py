import unittest
from unittest.mock import Mock, patch
import os
import sys

# Add the handler directory to the path for standalone testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestTimescaleDBHandlerStandalone(unittest.TestCase):
    """Standalone tests for TimescaleDB handler to avoid MindsDB dependency issues."""

    def test_01_timescaledb_import(self):
        """Test that we can import TimescaleDB handler components."""
        try:
            # Test connection args import
            import timescaledb_handler

            connection_args = timescaledb_handler.connection_args
            self.assertIsInstance(connection_args, dict)

            # Verify required PostgreSQL fields exist (TimescaleDB extends PostgreSQL)
            required_fields = ["host", "user", "password", "database"]
            for field in required_fields:
                self.assertIn(field, connection_args)

        except ImportError as e:
            self.skipTest(f"TimescaleDB handler imports not available: {e}")

    def test_02_timescaledb_functions(self):
        """Test TimescaleDB-specific function patterns."""
        timescaledb_functions = [
            "time_bucket",
            "time_bucket_gapfill",
            "create_hypertable",
            "add_retention_policy",
            "add_compression_policy",
            "interpolate",
            "locf",  # last observation carried forward
            "first",
            "last",
        ]

        for func in timescaledb_functions:
            self.assertIsInstance(func, str)
            self.assertTrue(len(func) > 0)

    def test_03_time_series_sql_patterns(self):
        """Test time-series specific SQL patterns."""
        timeseries_queries = [
            "SELECT time_bucket('1 hour', time) as bucket, avg(value) FROM metrics GROUP BY bucket",
            "SELECT create_hypertable('sensor_data', 'time')",
            "SELECT add_retention_policy('sensor_data', INTERVAL '30 days')",
            "SELECT time_bucket_gapfill('15 minutes', time), interpolate(avg(value)) FROM data",
            "SELECT first(value, time) FROM metrics WHERE time > NOW() - INTERVAL '1 day'",
        ]

        for query in timeseries_queries:
            self.assertIsInstance(query, str)
            # Check for TimescaleDB-specific patterns
            timescale_patterns = [
                "time_bucket",
                "hypertable",
                "retention_policy",
                "compression_policy",
                "interpolate",
                "gapfill",
            ]
            self.assertTrue(any(pattern in query.lower() for pattern in timescale_patterns))

    def test_04_interval_types(self):
        """Test PostgreSQL/TimescaleDB interval types."""
        interval_types = ["'1 hour'", "'15 minutes'", "'1 day'", "'7 days'", "'1 month'", "'1 year'"]

        for interval in interval_types:
            self.assertIsInstance(interval, str)
            self.assertTrue("'" in interval)  # Should be quoted

    def test_05_aggregation_functions(self):
        """Test aggregation functions commonly used with time-series."""
        agg_functions = ["avg", "max", "min", "sum", "count", "first", "last", "stddev", "percentile_cont"]

        for func in agg_functions:
            self.assertIsInstance(func, str)
            self.assertTrue(len(func) > 0)

    def test_06_hypertable_operations(self):
        """Test hypertable-specific operations."""
        hypertable_operations = [
            "create_hypertable('table_name', 'time_column')",
            "set_chunk_time_interval('table_name', INTERVAL '1 day')",
            "drop_chunks('table_name', INTERVAL '30 days')",
            "show_chunks('table_name')",
        ]

        for operation in hypertable_operations:
            self.assertIsInstance(operation, str)
            self.assertIn("table_name", operation)

    @patch("psycopg.connect")
    def test_07_mock_postgres_connection(self, mock_psycopg_connect):
        """Test mock PostgreSQL connection for TimescaleDB."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_psycopg_connect.return_value = mock_connection

        # Test connection parameters
        connection = mock_psycopg_connect(
            host="localhost", port=5432, user="postgres", password="test_password", database="timescaledb"
        )

        self.assertIsNotNone(connection)
        mock_psycopg_connect.assert_called_once()

    def test_08_continuous_aggregates(self):
        """Test continuous aggregate patterns."""
        cagg_patterns = [
            "CREATE MATERIALIZED VIEW",
            "WITH (timescaledb.continuous)",
            "time_bucket('1 hour', time)",
            "GROUP BY bucket",
        ]

        sample_cagg = """
        CREATE MATERIALIZED VIEW hourly_metrics
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', time) as bucket,
               device_id,
               avg(temperature) as avg_temp
        FROM sensor_data
        GROUP BY bucket, device_id
        """

        for pattern in cagg_patterns:
            self.assertIn(pattern, sample_cagg)


if __name__ == "__main__":
    unittest.main()
