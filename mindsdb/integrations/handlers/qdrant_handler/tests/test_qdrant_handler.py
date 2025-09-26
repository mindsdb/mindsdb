import unittest
from unittest.mock import Mock, patch
import os
import pandas as pd

from mindsdb.integrations.handlers.qdrant_handler.qdrant_handler import QdrantHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import FilterCondition, FilterOperator, TableField


class TestQdrantHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for all tests."""
        cls.test_connection_data = {
            "host": os.environ.get("MDB_TEST_QDRANT_HOST", "localhost"),
            "port": int(os.environ.get("MDB_TEST_QDRANT_PORT", "6333")),
            "api_key": os.environ.get("MDB_TEST_QDRANT_API_KEY", "test_key"),
            "collection_config": {
                "vectors": {"size": 384, "distance": "Cosine"},
                "optimizers_config": {"default_segment_number": 2},
            },
        }
        cls.handler_kwargs = {"connection_data": cls.test_connection_data}

    def setUp(self):
        """Set up for each test method."""
        # Mock the QdrantClient
        self.mock_client = Mock()

        # Mock collection info
        mock_collection_info = Mock()
        mock_collection_info.name = "test_collection"
        self.mock_client.get_collections.return_value.collections = [mock_collection_info]

        # Mock records for search/scroll results
        self.mock_records = [
            Mock(id="doc1", payload={"document": "Test document 1", "category": "test"}, score=0.95),
            Mock(id="doc2", payload={"document": "Test document 2", "category": "test"}, score=0.87),
        ]

    @patch("qdrant_client.QdrantClient")
    def test_01_handler_initialization(self, mock_qdrant_client):
        """Test QdrantHandler initialization."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        self.assertEqual(handler.name, "test_qdrant")
        self.assertTrue(handler.is_connected)
        mock_qdrant_client.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_02_connect_success(self, mock_qdrant_client):
        """Test successful connection to Qdrant."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        self.assertTrue(handler.is_connected)
        self.assertEqual(handler._client, self.mock_client)

    @patch("qdrant_client.QdrantClient")
    def test_03_connect_failure(self, mock_qdrant_client):
        """Test connection failure handling."""
        mock_qdrant_client.side_effect = Exception("Connection failed")

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Connection should fail but not crash
        self.assertFalse(handler.is_connected)

    @patch("qdrant_client.QdrantClient")
    def test_04_check_connection(self, mock_qdrant_client):
        """Test connection status checking."""
        mock_qdrant_client.return_value = self.mock_client
        self.mock_client.get_locks.return_value = []

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        response = handler.check_connection()
        self.assertTrue(response.success)
        self.mock_client.get_locks.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_05_create_collection(self, mock_qdrant_client):
        """Test creating a Qdrant collection."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)
        handler.create_table("test_vectors", if_not_exists=True)

        # Should call create_collection with collection config
        self.mock_client.create_collection.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_06_insert_vectors(self, mock_qdrant_client):
        """Test inserting vector data into Qdrant."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Test data with embeddings
        test_data = pd.DataFrame(
            [
                {
                    TableField.ID.value: "test1",
                    TableField.CONTENT.value: "Test document 1",
                    TableField.EMBEDDINGS.value: [0.1, 0.2, 0.3],
                    TableField.METADATA.value: {"category": "test", "source": "unit_test"},
                },
                {
                    TableField.ID.value: "test2",
                    TableField.CONTENT.value: "Test document 2",
                    TableField.EMBEDDINGS.value: [0.4, 0.5, 0.6],
                    TableField.METADATA.value: {"category": "test", "source": "unit_test"},
                },
            ]
        )

        handler.insert("test_vectors", test_data)

        # Should call upsert with proper batch structure
        self.mock_client.upsert.assert_called_once()
        call_args = self.mock_client.upsert.call_args
        self.assertEqual(call_args[0][0], "test_vectors")  # table_name
        self.assertIsNotNone(call_args[1]["points"])  # batch points

    @patch("qdrant_client.QdrantClient")
    def test_07_vector_similarity_search(self, mock_qdrant_client):
        """Test vector similarity search."""
        mock_qdrant_client.return_value = self.mock_client
        self.mock_client.search.return_value = self.mock_records

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Test vector search conditions
        conditions = [
            FilterCondition(column=TableField.SEARCH_VECTOR.value, op=FilterOperator.EQUAL, value=[0.1, 0.2, 0.3])
        ]

        result = handler.select("test_vectors", conditions=conditions, limit=5)

        self.assertIsInstance(result, pd.DataFrame)
        # Should execute similarity search
        self.mock_client.search.assert_called_once()
        call_args = self.mock_client.search.call_args
        self.assertEqual(call_args[0][0], "test_vectors")  # table_name
        self.assertEqual(call_args[1]["query_vector"], [0.1, 0.2, 0.3])

    @patch("qdrant_client.QdrantClient")
    def test_08_metadata_filtering(self, mock_qdrant_client):
        """Test filtering by metadata fields."""
        mock_qdrant_client.return_value = self.mock_client
        self.mock_client.scroll.return_value = (self.mock_records, None)

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Test metadata filtering
        conditions = [FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="documents")]

        result = handler.select("test_vectors", conditions=conditions)

        self.assertIsInstance(result, pd.DataFrame)
        # Should call scroll with metadata filters
        self.mock_client.scroll.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_09_id_based_retrieval(self, mock_qdrant_client):
        """Test retrieving vectors by ID."""
        mock_qdrant_client.return_value = self.mock_client
        self.mock_client.retrieve.return_value = self.mock_records

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Test ID-based retrieval
        conditions = [FilterCondition(column=TableField.ID.value, op=FilterOperator.EQUAL, value=["doc1", "doc2"])]

        result = handler.select("test_vectors", conditions=conditions)

        self.assertIsInstance(result, pd.DataFrame)
        # Should call retrieve with IDs
        self.mock_client.retrieve.assert_called_once()
        call_args = self.mock_client.retrieve.call_args
        self.assertEqual(call_args[0][0], "test_vectors")  # table_name

    @patch("qdrant_client.QdrantClient")
    def test_10_combined_vector_metadata_search(self, mock_qdrant_client):
        """Test combining vector search with metadata filtering."""
        mock_qdrant_client.return_value = self.mock_client
        self.mock_client.search.return_value = self.mock_records

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Combined conditions
        conditions = [
            FilterCondition(column=TableField.SEARCH_VECTOR.value, op=FilterOperator.EQUAL, value=[0.1, 0.2, 0.3]),
            FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="documents"),
        ]

        result = handler.select("test_vectors", conditions=conditions, limit=10)

        self.assertIsInstance(result, pd.DataFrame)
        # Should call search with both vector and metadata filters
        self.mock_client.search.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_11_delete_by_id(self, mock_qdrant_client):
        """Test deleting vectors by ID."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        conditions = [
            FilterCondition(column=TableField.ID.value, op=FilterOperator.EQUAL, value=["test_id1", "test_id2"])
        ]

        handler.delete("test_vectors", conditions)

        # Should call delete with point selector
        self.mock_client.delete.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_12_delete_by_metadata(self, mock_qdrant_client):
        """Test deleting vectors by metadata conditions."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        conditions = [FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="test_category")]

        handler.delete("test_vectors", conditions)

        # Should call delete with filter selector
        self.mock_client.delete.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_13_get_collections(self, mock_qdrant_client):
        """Test getting list of collections."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        response = handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.mock_client.get_collections.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_14_get_columns(self, mock_qdrant_client):
        """Test getting columns for a collection."""
        mock_qdrant_client.return_value = self.mock_client
        mock_collection = Mock()
        self.mock_client.get_collection.return_value = mock_collection

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        response = handler.get_columns("test_vectors")

        # Should return standard vector store columns
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.mock_client.get_collection.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_15_update_vectors(self, mock_qdrant_client):
        """Test updating vectors (should use upsert)."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Test update data
        test_data = pd.DataFrame(
            [
                {
                    TableField.ID.value: "test1",
                    TableField.CONTENT.value: "Updated test document",
                    TableField.EMBEDDINGS.value: [0.7, 0.8, 0.9],
                    TableField.METADATA.value: {"category": "updated"},
                }
            ]
        )

        handler.update("test_vectors", test_data)

        # Update should call upsert
        self.mock_client.upsert.assert_called_once()

    @patch("qdrant_client.QdrantClient")
    def test_16_filter_operator_mapping(self, mock_qdrant_client):
        """Test different filter operators."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Test different operators
        test_operators = [
            (FilterOperator.EQUAL, "test_value"),
            (FilterOperator.NOT_EQUAL, "test_value"),
            (FilterOperator.LESS_THAN, 100),
            (FilterOperator.LESS_THAN_OR_EQUAL, 100),
            (FilterOperator.GREATER_THAN, 50),
            (FilterOperator.GREATER_THAN_OR_EQUAL, 50),
        ]

        for operator, value in test_operators:
            filter_dict = handler._get_qdrant_filter(operator, value)
            self.assertIsInstance(filter_dict, dict)
            # Should contain either 'match' or 'range' key
            self.assertTrue("match" in filter_dict or "range" in filter_dict)

    @patch("qdrant_client.QdrantClient")
    def test_17_drop_collection(self, mock_qdrant_client):
        """Test dropping a collection."""
        mock_qdrant_client.return_value = self.mock_client
        self.mock_client.delete_collection.return_value = True

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        handler.drop_table("test_vectors", if_exists=True)

        # Should call delete_collection
        self.mock_client.delete_collection.assert_called_once_with("test_vectors")

    @patch("qdrant_client.QdrantClient")
    def test_18_scroll_without_conditions(self, mock_qdrant_client):
        """Test scrolling through all vectors without conditions."""
        mock_qdrant_client.return_value = self.mock_client
        self.mock_client.scroll.return_value = (self.mock_records, None)

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        result = handler.select("test_vectors", limit=10, offset=0)

        self.assertIsInstance(result, pd.DataFrame)
        # Should call scroll without filters
        self.mock_client.scroll.assert_called_once_with("test_vectors", limit=10, offset=0)

    @patch("qdrant_client.QdrantClient")
    def test_19_error_handling_unsupported_operator(self, mock_qdrant_client):
        """Test error handling for unsupported filter operators."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)

        # Test unsupported operator
        with self.assertRaises(Exception) as context:
            handler._get_qdrant_filter(FilterOperator.LIKE, "test_value")

        self.assertIn("not supported", str(context.exception))

    @patch("qdrant_client.QdrantClient")
    def test_20_disconnect(self, mock_qdrant_client):
        """Test disconnecting from Qdrant."""
        mock_qdrant_client.return_value = self.mock_client

        handler = QdrantHandler("test_qdrant", **self.handler_kwargs)
        self.assertTrue(handler.is_connected)

        handler.disconnect()

        self.assertFalse(handler.is_connected)
        self.mock_client.close.assert_called_once()

    def test_21_connection_config_validation(self):
        """Test connection configuration validation."""
        # Test that collection_config is required
        invalid_config = self.test_connection_data.copy()
        invalid_config.pop("collection_config")

        with self.assertRaises(KeyError):
            QdrantHandler("test_qdrant", connection_data=invalid_config)

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, "mock_client"):
            self.mock_client.reset_mock()


if __name__ == "__main__":
    unittest.main()
