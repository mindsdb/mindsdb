import pytest
import unittest
from unittest.mock import patch, MagicMock
from datetime import date

try:
    from cassandra.cluster import NoHostAvailable
    from mindsdb.integrations.handlers.cassandra_handler.cassandra_handler import (
        CassandraHandler,
    )
    from cassandra.util import Date
except ImportError:
    pytestmark = pytest.mark.skip("Cassandra handler not installed")

from base_handler_test import BaseDatabaseHandlerTest


class TestCassandraHandler(BaseDatabaseHandlerTest, unittest.TestCase):
    """Unit tests for CassandraHandler"""

    @property
    def dummy_connection_data(self):
        """Dummy connection data for testing"""
        return {
            "host": "localhost",
            "port": 9042,
            "user": "test_user",
            "password": "test_password",
            "keyspace": "test_keyspace",
        }

    @property
    def mock_keyspace(self):
        """Mock keyspace for testing"""
        return self.dummy_connection_data["keyspace"]

    @property
    def err_to_raise_on_connect_failure(self):
        """Exception to raise on connection failure"""
        return NoHostAvailable("Connection failed", {})

    @property
    def get_tables_query(self):
        """Query to get tables"""
        keyspace = self.mock_keyspace
        return (
            f"SELECT table_name FROM system_schema.tables "
            f"WHERE keyspace_name = '{keyspace}'"
        )

    @property
    def get_columns_query(self):
        """Query to get columns"""
        keyspace = self.mock_keyspace
        table = self.mock_table
        return (
            f"SELECT column_name, type FROM system_schema.columns "
            f"WHERE keyspace_name = '{keyspace}' "
            f"AND table_name = '{table}'"
        )

    def create_handler(self):
        """Create a Cassandra handler instance"""
        return CassandraHandler("cassandra", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        """Create patcher for Cassandra connection"""
        return patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        )

    # Connection Tests
    def test_connect_success(self):
        """Test successful connection to Cassandra"""
        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster:
            mock_session = MagicMock()
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = mock_session
            mock_cluster.return_value = mock_cluster_instance

            self.handler.connect()

            mock_cluster.assert_called_once()
            mock_cluster_instance.connect.assert_called_once_with(self.mock_keyspace)
            self.assertTrue(self.handler.is_connected)
            self.assertEqual(self.handler.session, mock_session)

    def test_connect_with_auth(self):
        """Test connection with authentication"""
        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster, patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.PlainTextAuthProvider"
        ) as mock_auth:

            mock_session = MagicMock()
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = mock_session
            mock_cluster.return_value = mock_cluster_instance
            mock_auth_instance = MagicMock()
            mock_auth.return_value = mock_auth_instance

            self.handler.connect()

            mock_auth.assert_called_once_with(
                username=self.dummy_connection_data["user"],
                password=self.dummy_connection_data["password"],
            )
            mock_cluster.assert_called_once()
            call_kwargs = mock_cluster.call_args[1]
            self.assertEqual(call_kwargs["auth_provider"], mock_auth_instance)
            self.assertEqual(call_kwargs["contact_points"], ["localhost"])
            self.assertEqual(call_kwargs["port"], 9042)
            self.assertEqual(call_kwargs["protocol_version"], 4)

    def test_connect_without_keyspace(self):
        """Test connection without specifying a keyspace"""
        connection_data = self.dummy_connection_data.copy()
        del connection_data["keyspace"]
        handler = CassandraHandler("cassandra", connection_data=connection_data)

        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster:
            mock_session = MagicMock()
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = mock_session
            mock_cluster.return_value = mock_cluster_instance

            handler.connect()

            mock_cluster_instance.connect.assert_called_once_with(None)

    def test_connect_missing_credentials(self):
        """Test connection fails when credentials are incomplete"""
        # Missing password
        connection_data = self.dummy_connection_data.copy()
        del connection_data["password"]
        handler = CassandraHandler("cassandra", connection_data=connection_data)

        with self.assertRaises(ValueError):
            handler.connect()

        # Missing user
        connection_data = self.dummy_connection_data.copy()
        del connection_data["user"]
        handler = CassandraHandler("cassandra", connection_data=connection_data)

        with self.assertRaises(ValueError):
            handler.connect()

    def test_connect_with_secure_bundle(self):
        """Test connection using secure connect bundle (Astra DB)"""
        connection_data = self.dummy_connection_data.copy()
        connection_data["secure_connect_bundle"] = "/path/to/bundle.zip"
        handler = CassandraHandler("cassandra", connection_data=connection_data)

        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster:
            mock_session = MagicMock()
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = mock_session
            mock_cluster.return_value = mock_cluster_instance

            handler.connect()

            call_kwargs = mock_cluster.call_args[1]
            self.assertIn("cloud", call_kwargs)
            self.assertEqual(
                call_kwargs["cloud"]["secure_connect_bundle"], "/path/to/bundle.zip"
            )

    def test_connect_reuses_existing_connection(self):
        """Test that connect reuses existing connection if already connected"""
        mock_session = MagicMock()
        self.handler.session = mock_session
        self.handler.is_connected = True

        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster:
            session = self.handler.connect()

            mock_cluster.assert_not_called()
            self.assertEqual(session, mock_session)

    def test_check_connection_success(self):
        """Test check_connection returns success when connection is valid"""
        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster:
            mock_session = MagicMock()
            mock_result = MagicMock()
            mock_result.one.return_value = ("4.1.0",)
            mock_session.execute.return_value = mock_result
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = mock_session
            mock_cluster.return_value = mock_cluster_instance

            response = self.handler.check_connection()

            self.assertTrue(response.success)
            self.assertIsNone(response.error_message)
            mock_session.execute.assert_called_once_with(
                "SELECT release_version FROM system.local"
            )

    def test_check_connection_failure(self):
        """Test check_connection returns failure when connection fails"""
        error_msg = "No host available"
        error = NoHostAvailable(error_msg, {})

        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster:
            mock_cluster.side_effect = error

            response = self.handler.check_connection()

            self.assertFalse(response.success)
            self.assertIsNotNone(response.error_message)

    def test_native_query(self):
        """Test cassandra native query"""
        with patch(
            "mindsdb.integrations.handlers.cassandra_handler."
            "cassandra_handler.Cluster"
        ) as mock_cluster:
            mock_session = MagicMock()
            mock_row = MagicMock()
            mock_cassandra_date = MagicMock(spec=Date)
            mock_cassandra_date.date.return_value = date(2023, 1, 1)

            mock_row._asdict.return_value = {
                "id": 1,
                "name": "test",
                "created_at": mock_cassandra_date,
            }
            mock_response = [mock_row]
            mock_session.execute.return_value.all.return_value = mock_response
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = mock_session
            mock_cluster.return_value = mock_cluster_instance

            response = self.handler.native_query("SELECT * FROM test_table")

            self.assertEqual(response.error_code, 0)
            self.assertEqual(len(response.data_frame), 1)
            self.assertEqual(response.data_frame.iloc[0]["id"], 1)
            self.assertEqual(response.data_frame.iloc[0]["name"], "test")
            self.assertIsInstance(response.data_frame.iloc[0]["created_at"], date)

    def test_get_columns(self):
        """Cassandra handler doesn't implement get_columns yet"""
        self.skipTest("get_columns not implemented in CassandraHandler")

    def test_get_tables(self):
        """Test get_tables uses DESCRIBE TABLES command"""
        self.handler.native_query = MagicMock()
        self.handler.get_tables()

        # Cassandra handler uses DESCRIBE TABLES instead of system schema
        self.handler.native_query.assert_called_once_with("DESCRIBE TABLES;")


if __name__ == "__main__":
    unittest.main()
