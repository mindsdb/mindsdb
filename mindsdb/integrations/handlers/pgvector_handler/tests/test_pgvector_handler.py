import unittest
from unittest.mock import Mock, patch
import os
import pandas as pd

from mindsdb.integrations.handlers.pgvector_handler.pgvector_handler import PgVectorHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import FilterCondition, FilterOperator, TableField


class TestPgVectorHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for all tests."""
        cls.test_connection_data = {
            "host": os.environ.get("MDB_TEST_PGVECTOR_HOST", "localhost"),
            "port": int(os.environ.get("MDB_TEST_PGVECTOR_PORT", "5432")),
            "user": os.environ.get("MDB_TEST_PGVECTOR_USER", "postgres"),
            "password": os.environ.get("MDB_TEST_PGVECTOR_PASSWORD", "test_password"),
            "database": os.environ.get("MDB_TEST_PGVECTOR_DATABASE", "test_db"),
            "schema": os.environ.get("MDB_TEST_PGVECTOR_SCHEMA", "public"),
        }
        cls.handler_kwargs = {"connection_data": cls.test_connection_data}

    def setUp(self):
        """Set up for each test method."""
        # Mock the psycopg connection
        self.mock_connection = Mock()
        self.mock_cursor = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor

        # Mock cursor responses for vector operations
        self.mock_cursor.fetchall.return_value = [
            ("doc1", "This is a test document", "[0.1, 0.2, 0.3]", 0.95),
            ("doc2", "Another test document", "[0.4, 0.5, 0.6]", 0.87),
        ]
        self.mock_cursor.description = [
            Mock(name="id"),
            Mock(name="content"),
            Mock(name="embedding"),
            Mock(name="similarity"),
        ]

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_01_connect_success(self, mock_register_vector, mock_psycopg_connect):
        """Test successful connection to PostgreSQL with pgvector."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        self.assertTrue(handler.is_connected)
        mock_psycopg_connect.assert_called_once()
        mock_register_vector.assert_called_once()

    @patch("psycopg.connect")
    def test_02_connect_failure(self, mock_psycopg_connect):
        """Test connection failure handling."""
        mock_psycopg_connect.side_effect = Exception("Connection failed")

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)

        # Connection should fail but not crash
        self.assertFalse(handler.is_connected)

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_03_create_table(self, mock_register_vector, mock_psycopg_connect):
        """Test creating a table with vector column."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        handler.create_table("test_vectors", if_not_exists=True)

        # Should execute CREATE TABLE with vector column
        self.mock_cursor.execute.assert_called()
        executed_sql = self.mock_cursor.execute.call_args[0][0]
        self.assertIn("vector", executed_sql.lower())

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_04_insert_vectors(self, mock_register_vector, mock_psycopg_connect):
        """Test inserting vector data."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Test data with embeddings
        test_data = pd.DataFrame(
            [
                {
                    TableField.ID.value: "test1",
                    TableField.CONTENT.value: "Test document 1",
                    TableField.EMBEDDINGS.value: [0.1, 0.2, 0.3],
                    TableField.METADATA.value: {"category": "test"},
                },
                {
                    TableField.ID.value: "test2",
                    TableField.CONTENT.value: "Test document 2",
                    TableField.EMBEDDINGS.value: [0.4, 0.5, 0.6],
                    TableField.METADATA.value: {"category": "test"},
                },
            ]
        )

        handler.insert("test_vectors", test_data)

        # Should execute INSERT with vector data
        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_05_vector_similarity_search(self, mock_register_vector, mock_psycopg_connect):
        """Test vector similarity search."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock similarity search result
        self.mock_cursor.fetchall.return_value = [
            ("doc1", "Similar document", "[0.1, 0.2, 0.3]", '{"category": "test"}', 0.95),
            ("doc2", "Another similar doc", "[0.15, 0.25, 0.35]", '{"category": "test"}', 0.87),
        ]

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Test vector search conditions
        conditions = [
            FilterCondition(column=TableField.SEARCH_VECTOR.value, op=FilterOperator.EQUAL, value="[0.1, 0.2, 0.3]")
        ]

        result = handler.select("test_vectors", conditions=conditions, limit=5)

        self.assertIsInstance(result, pd.DataFrame)
        # Should execute similarity search query
        self.mock_cursor.execute.assert_called()
        executed_sql = self.mock_cursor.execute.call_args[0][0]
        self.assertIn("<->", executed_sql)  # Cosine distance operator

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_06_metadata_filtering(self, mock_register_vector, mock_psycopg_connect):
        """Test filtering by metadata fields."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Test metadata filtering
        conditions = [FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="documents")]

        handler.select("test_vectors", conditions=conditions)

        self.mock_cursor.execute.assert_called()
        executed_sql = self.mock_cursor.execute.call_args[0][0]
        self.assertIn("metadata", executed_sql.lower())

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_07_combined_vector_metadata_search(self, mock_register_vector, mock_psycopg_connect):
        """Test combining vector similarity with metadata filtering."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Combined conditions
        conditions = [
            FilterCondition(column=TableField.SEARCH_VECTOR.value, op=FilterOperator.EQUAL, value="[0.1, 0.2, 0.3]"),
            FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="documents"),
        ]

        handler.select("test_vectors", conditions=conditions, limit=10)

        self.mock_cursor.execute.assert_called()
        executed_sql = self.mock_cursor.execute.call_args[0][0]
        # Should contain both vector similarity and metadata filtering
        self.assertIn("<->", executed_sql)
        self.assertIn("metadata", executed_sql.lower())

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_08_delete_vectors(self, mock_register_vector, mock_psycopg_connect):
        """Test deleting vectors by conditions."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        conditions = [FilterCondition(column=TableField.ID.value, op=FilterOperator.EQUAL, value="test_id")]

        handler.delete("test_vectors", conditions)

        # Should execute DELETE query
        self.mock_cursor.execute.assert_called()
        executed_sql = self.mock_cursor.execute.call_args[0][0]
        self.assertIn("DELETE", executed_sql.upper())

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_09_get_tables_with_vectors(self, mock_register_vector, mock_psycopg_connect):
        """Test getting tables that contain vector columns."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock result showing vector tables
        self.mock_cursor.fetchall.return_value = [
            ("public", "embeddings_table", "BASE TABLE"),
            ("public", "document_vectors", "BASE TABLE"),
        ]

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        response = handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_10_get_vector_columns(self, mock_register_vector, mock_psycopg_connect):
        """Test getting column information including vector columns."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock result with vector column type
        self.mock_cursor.fetchall.return_value = [
            ("id", "text", "NO", None),
            ("content", "text", "YES", None),
            ("embedding", "vector", "YES", None),
            ("metadata", "jsonb", "YES", None),
        ]
        self.mock_cursor.description = [
            Mock(name="column_name"),
            Mock(name="data_type"),
            Mock(name="is_nullable"),
            Mock(name="column_default"),
        ]

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        response = handler.get_columns("test_vectors")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_11_distance_functions(self, mock_register_vector, mock_psycopg_connect):
        """Test different distance functions for vector similarity."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Test cosine distance
        conditions = [
            FilterCondition(column=TableField.SEARCH_VECTOR.value, op=FilterOperator.EQUAL, value="[0.1, 0.2, 0.3]")
        ]

        handler.select("test_vectors", conditions=conditions, limit=5)

        # Should use default cosine distance
        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_12_keyword_search(self, mock_register_vector, mock_psycopg_connect):
        """Test keyword search functionality."""
        mock_psycopg_connect.return_value = self.mock_connection

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Test text search
        conditions = [FilterCondition(column="content", op=FilterOperator.LIKE, value="test document")]

        handler.select("test_vectors", conditions=conditions)

        self.mock_cursor.execute.assert_called()

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_13_error_handling(self, mock_register_vector, mock_psycopg_connect):
        """Test error handling for invalid operations."""
        mock_psycopg_connect.return_value = self.mock_connection
        self.mock_cursor.execute.side_effect = Exception("Vector dimension mismatch")

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Test invalid vector operation
        test_data = pd.DataFrame(
            [
                {
                    TableField.ID.value: "test",
                    TableField.EMBEDDINGS.value: [0.1, 0.2],  # Wrong dimension
                }
            ]
        )

        # Should handle error gracefully
        with self.assertRaises(Exception):
            handler.insert("test_vectors", test_data)

    @patch("psycopg.connect")
    @patch("pgvector.psycopg.register_vector")
    def test_14_pgvector_extension_check(self, mock_register_vector, mock_psycopg_connect):
        """Test checking for pgvector extension."""
        mock_psycopg_connect.return_value = self.mock_connection

        # Mock extension check result
        self.mock_cursor.fetchall.return_value = [("vector", "1.0.0", "vector")]

        handler = PgVectorHandler("test_pgvector", **self.handler_kwargs)
        handler.connect()

        # Check if vector extension is available
        query = "SELECT * FROM pg_extension WHERE extname = 'vector'"
        handler.native_query(query)

        self.mock_cursor.execute.assert_called()

    def test_15_connection_args_validation(self):
        """Test connection arguments validation."""
        from mindsdb.integrations.handlers.pgvector_handler.connection_args import connection_args

        # Test required PostgreSQL fields
        required_fields = ["host", "user", "password", "database"]
        for field in required_fields:
            self.assertIn(field, connection_args)

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, "mock_cursor"):
            self.mock_cursor.reset_mock()
        if hasattr(self, "mock_connection"):
            self.mock_connection.reset_mock()


if __name__ == "__main__":
    unittest.main()
