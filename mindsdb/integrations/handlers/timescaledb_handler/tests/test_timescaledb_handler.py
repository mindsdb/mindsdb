import unittest
from unittest.mock import Mock, patch
import os
import pandas as pd

from mindsdb.integrations.handlers.timescaledb_handler.timescaledb_handler import TimeScaleDBHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class TestTimeScaleDBHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for all tests."""
        cls.test_connection_data = {
            "host": os.environ.get("MDB_TEST_TIMESCALEDB_HOST", "localhost"),
            "port": int(os.environ.get("MDB_TEST_TIMESCALEDB_PORT", "5432")),
            "user": os.environ.get("MDB_TEST_TIMESCALEDB_USER", "postgres"),
            "password": os.environ.get("MDB_TEST_TIMESCALEDB_PASSWORD", "test_password"),
            "database": os.environ.get("MDB_TEST_TIMESCALEDB_DATABASE", "timescaledb"),
            "schema": os.environ.get("MDB_TEST_TIMESCALEDB_SCHEMA", "public"),
        }
        cls.handler_kwargs = {"connection_data": cls.test_connection_data}

    def setUp(self):
        """Set up for each test method."""
        # Mock the psycopg connection
        self.mock_connection = Mock()
        self.mock_cursor = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor

        # Mock cursor responses for time-series operations
        self.mock_cursor.fetchall.return_value = [
            ("cpu_metrics", "BASE TABLE", "2023-01-01 10:00:00"),
            ("temperature_data", "BASE TABLE", "2023-01-01 11:00:00"),
        ]
        self.mock_cursor.description = [Mock(name="table_name"), Mock(name="table_type"), Mock(name="created")]

    @patch("psycopg.connect")
    def test_01_connect_success(self, mock_psycopg_connect):
        """Test successful connection to TimescaleDB."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        self.assertTrue(handler.is_connected)
        mock_psycopg_connect.assert_called_once()

    @patch("psycopg.connect")
    def test_02_connect_failure(self, mock_psycopg_connect):
        """Test connection failure handling."""
        mock_psycopg_connect.side_effect = Exception("Connection failed")

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)

        # Connection should fail but not crash
        self.assertFalse(handler.is_connected)

    @patch("psycopg.connect")
    def test_03_check_connection(self, mock_psycopg_connect):
        """Test connection status checking."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        response = handler.check_connection()
        self.assertTrue(response.success)

    @patch("psycopg.connect")
    def test_04_native_query_select(self, mock_psycopg_connect):
        """Test native query execution for SELECT statements."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock time-series query result
        self.mock_cursor.fetchall.return_value = [
            ("2023-01-01 10:00:00", "server1", 75.5, "cpu"),
            ("2023-01-01 10:01:00", "server1", 78.2, "cpu"),
        ]
        self.mock_cursor.description = [Mock(name="time"), Mock(name="host"), Mock(name="value"), Mock(name="metric")]

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        query = "SELECT time, host, value, metric FROM cpu_metrics WHERE time >= '2023-01-01' LIMIT 2"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)

    @patch("psycopg.connect")
    def test_05_time_series_aggregation(self, mock_psycopg_connect):
        """Test TimescaleDB time-series aggregation functions."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock aggregation query result
        self.mock_cursor.fetchall.return_value = [
            ("2023-01-01 10:00:00", 75.5, 80.2, 77.8),
            ("2023-01-01 11:00:00", 73.1, 79.5, 76.3),
        ]
        self.mock_cursor.description = [
            Mock(name="time_bucket"),
            Mock(name="avg_value"),
            Mock(name="max_value"),
            Mock(name="min_value"),
        ]

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test time_bucket aggregation
        query = """
        SELECT time_bucket('1 hour', time) as time_bucket,
               avg(value) as avg_value,
               max(value) as max_value,
               min(value) as min_value
        FROM cpu_metrics
        WHERE time >= '2023-01-01'
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("psycopg.connect")
    def test_06_hypertable_operations(self, mock_psycopg_connect):
        """Test TimescaleDB hypertable-specific operations."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test create hypertable
        create_hypertable_query = """
        SELECT create_hypertable('sensor_data', 'time', 
                                chunk_time_interval => INTERVAL '1 day')
        """
        handler.native_query(create_hypertable_query)

        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    def test_07_continuous_aggregates(self, mock_psycopg_connect):
        """Test TimescaleDB continuous aggregates."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test continuous aggregate creation
        cagg_query = """
        CREATE MATERIALIZED VIEW hourly_metrics
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', time) as bucket,
               device_id,
               avg(temperature) as avg_temp
        FROM sensor_data
        GROUP BY bucket, device_id
        """
        handler.native_query(cagg_query)

        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    def test_08_data_retention_policy(self, mock_psycopg_connect):
        """Test TimescaleDB data retention policies."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test retention policy
        retention_query = """
        SELECT add_retention_policy('sensor_data', INTERVAL '30 days')
        """
        handler.native_query(retention_query)

        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    def test_09_compression_settings(self, mock_psycopg_connect):
        """Test TimescaleDB compression features."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test compression policy
        compression_query = """
        SELECT add_compression_policy('sensor_data', INTERVAL '7 days')
        """
        handler.native_query(compression_query)

        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    def test_10_time_bucket_gapfill(self, mock_psycopg_connect):
        """Test TimescaleDB gapfill functionality."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock gapfill query result
        self.mock_cursor.fetchall.return_value = [
            ("2023-01-01 10:00:00", 75.5),
            ("2023-01-01 10:15:00", None),  # Gap filled
            ("2023-01-01 10:30:00", 76.8),
        ]
        self.mock_cursor.description = [Mock(name="time"), Mock(name="interpolated_value")]

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test gapfill with interpolation
        gapfill_query = """
        SELECT time_bucket_gapfill('15 minutes', time) as time,
               interpolate(avg(value)) as interpolated_value
        FROM sensor_data
        WHERE time >= '2023-01-01 10:00:00' AND time < '2023-01-01 11:00:00'
        GROUP BY time
        ORDER BY time
        """
        response = handler.native_query(gapfill_query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("psycopg.connect")
    def test_11_get_tables(self, mock_psycopg_connect):
        """Test getting list of tables including hypertables."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        response = handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("psycopg.connect")
    def test_12_get_columns(self, mock_psycopg_connect):
        """Test getting columns for a specific table."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock column information
        self.mock_cursor.fetchall.return_value = [
            ("time", "timestamp with time zone", "NO", "", None),
            ("device_id", "integer", "NO", "", None),
            ("temperature", "double precision", "YES", "", None),
            ("humidity", "double precision", "YES", "", None),
        ]
        self.mock_cursor.description = [
            Mock(name="column_name"),
            Mock(name="data_type"),
            Mock(name="is_nullable"),
            Mock(name="column_default"),
            Mock(name="character_maximum_length"),
        ]

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        response = handler.get_columns("sensor_data")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("psycopg.connect")
    def test_13_timescaledb_functions(self, mock_psycopg_connect):
        """Test TimescaleDB-specific functions."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock function result
        self.mock_cursor.fetchall.return_value = [("1.7.4",), ("sensor_data", "time", "1 day")]

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test TimescaleDB version
        version_query = "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        handler.native_query(version_query)

        # Test hypertable info
        hypertable_query = """
        SELECT hypertable_name, dimension_column_name, interval_length
        FROM timescaledb_information.dimensions
        """
        handler.native_query(hypertable_query)

        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    def test_14_error_handling(self, mock_psycopg_connect):
        """Test error handling for invalid operations."""
        mock_psycopg_connect.return_value = self.mock_connection
        self.mock_cursor.execute.side_effect = Exception("Hypertable does not exist")

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test invalid hypertable operation
        query = "SELECT create_hypertable('non_existent_table', 'time')"
        response = handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIsNotNone(response.error_message)

    def test_15_connection_args_validation(self):
        """Test connection arguments validation."""
        from mindsdb.integrations.handlers.timescaledb_handler.timescaledb_handler import connection_args

        # Test required PostgreSQL fields for TimescaleDB
        required_fields = ["host", "user", "password", "database"]
        for field in required_fields:
            self.assertIn(field, connection_args)

    @patch("psycopg.connect")
    def test_16_schema_support(self, mock_psycopg_connect):
        """Test schema-specific operations."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = TimeScaleDBHandler("test_timescaledb", **self.handler_kwargs)
        handler.connect()

        # Test schema-specific query
        schema_query = "SELECT * FROM public.sensor_data LIMIT 1"
        handler.native_query(schema_query)

        self.mock_cursor.execute.assert_called()

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, "mock_cursor"):
            self.mock_cursor.reset_mock()
        if hasattr(self, "mock_connection"):
            self.mock_connection.reset_mock()


if __name__ == "__main__":
    unittest.main()
