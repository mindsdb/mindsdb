import unittest
from unittest.mock import Mock, patch
import os
import pandas as pd

from mindsdb.integrations.handlers.pinecone_handler.pinecone_handler import PineconeHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import FilterCondition, FilterOperator, TableField


class TestPineconeHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for all tests."""
        cls.test_connection_data = {
            "api_key": os.environ.get("MDB_TEST_PINECONE_API_KEY", "test-api-key-123"),
            "environment": os.environ.get("MDB_TEST_PINECONE_ENVIRONMENT", "gcp-starter"),
            "dimension": int(os.environ.get("MDB_TEST_PINECONE_DIMENSION", "384")),
            "metric": os.environ.get("MDB_TEST_PINECONE_METRIC", "cosine"),
            "spec": {"cloud": "aws", "region": "us-east-1"},
        }
        cls.handler_kwargs = {"connection_data": cls.test_connection_data}

    def setUp(self):
        """Set up for each test method."""
        # Mock the Pinecone client and index
        self.mock_client = Mock()
        self.mock_index = Mock()

        # Mock index operations
        self.mock_index.describe_index_stats.return_value = {
            "dimension": 384,
            "index_fullness": 0.0,
            "namespaces": {},
            "total_vector_count": 0,
        }
        self.mock_client.Index.return_value = self.mock_index

        # Mock index listing
        self.mock_client.list_indexes.return_value = [
            {"name": "test-index", "dimension": 384, "metric": "cosine"},
            {"name": "demo-index", "dimension": 512, "metric": "euclidean"},
        ]

        # Mock query results
        self.mock_query_result = {
            "matches": [
                Mock(
                    id="doc1", score=0.95, values=[0.1, 0.2, 0.3], metadata={"category": "test", "source": "unit_test"}
                ),
                Mock(
                    id="doc2", score=0.87, values=[0.4, 0.5, 0.6], metadata={"category": "test", "source": "unit_test"}
                ),
            ]
        }

        # Configure mock to_dict method for query results
        for match in self.mock_query_result["matches"]:
            match.to_dict.return_value = {
                "id": match.id,
                "score": match.score,
                "values": match.values,
                "metadata": match.metadata,
            }

        self.mock_index.query.return_value = self.mock_query_result

    @patch("pinecone.Pinecone")
    def test_01_handler_initialization(self, mock_pinecone_class):
        """Test PineconeHandler initialization."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        self.assertEqual(handler.name, "test_pinecone")
        self.assertEqual(handler.connection_data, self.test_connection_data)
        self.assertFalse(handler.is_connected)

    @patch("pinecone.Pinecone")
    def test_02_connect_success(self, mock_pinecone_class):
        """Test successful connection to Pinecone."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        connection = handler.connect()

        self.assertEqual(connection, self.mock_client)
        mock_pinecone_class.assert_called_once_with(api_key=self.test_connection_data["api_key"])

    @patch("pinecone.Pinecone")
    def test_03_connect_missing_api_key(self, mock_pinecone_class):
        """Test connection failure when API key is missing."""
        invalid_config = self.test_connection_data.copy()
        del invalid_config["api_key"]

        handler = PineconeHandler("test_pinecone", connection_data=invalid_config)

        with self.assertRaises(ValueError) as context:
            handler.connect()

        self.assertIn("Required parameter (api_key)", str(context.exception))

    @patch("pinecone.Pinecone")
    def test_04_connect_failure(self, mock_pinecone_class):
        """Test connection failure handling."""
        mock_pinecone_class.side_effect = Exception("API connection failed")

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        result = handler.connect()

        self.assertIsNone(result)
        self.assertFalse(handler.is_connected)

    @patch("pinecone.Pinecone")
    def test_05_check_connection(self, mock_pinecone_class):
        """Test connection status checking."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        response = handler.check_connection()

        self.assertTrue(response.success)
        self.mock_client.list_indexes.assert_called_once()

    @patch("pinecone.Pinecone")
    def test_06_check_connection_failure(self, mock_pinecone_class):
        """Test connection check failure."""
        mock_pinecone_class.return_value = self.mock_client
        self.mock_client.list_indexes.side_effect = Exception("Connection failed")

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        response = handler.check_connection()

        self.assertFalse(response.success)
        self.assertIsNotNone(response.error_message)

    @patch("pinecone.Pinecone")
    def test_07_get_tables(self, mock_pinecone_class):
        """Test getting list of indexes (tables)."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        response = handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertIn("table_name", response.data_frame.columns)
        self.assertEqual(len(response.data_frame), 2)

    @patch("pinecone.Pinecone")
    @patch("pinecone.ServerlessSpec")
    def test_08_create_table(self, mock_serverless_spec, mock_pinecone_class):
        """Test creating a Pinecone index."""
        mock_pinecone_class.return_value = self.mock_client
        mock_spec_instance = Mock()
        mock_serverless_spec.return_value = mock_spec_instance

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        handler.create_table("test_index", if_not_exists=True)

        # Should call create_index with proper parameters
        self.mock_client.create_index.assert_called_once()
        call_args = self.mock_client.create_index.call_args
        self.assertEqual(call_args[1]["name"], "test_index")
        self.assertEqual(call_args[1]["dimension"], 384)
        self.assertEqual(call_args[1]["metric"], "cosine")

    @patch("pinecone.Pinecone")
    def test_09_create_table_already_exists(self, mock_pinecone_class):
        """Test creating index that already exists."""
        from pinecone.core.openapi.shared.exceptions import PineconeApiException

        mock_pinecone_class.return_value = self.mock_client
        mock_error = PineconeApiException(status=409, message="Index already exists")
        self.mock_client.create_index.side_effect = mock_error

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        # Should not raise exception when if_not_exists=True
        handler.create_table("test_index", if_not_exists=True)

        self.mock_client.create_index.assert_called_once()

    @patch("pinecone.Pinecone")
    def test_10_insert_vectors(self, mock_pinecone_class):
        """Test inserting vector data."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test data with embeddings
        test_data = pd.DataFrame(
            [
                {
                    TableField.ID.value: "test1",
                    TableField.EMBEDDINGS.value: [0.1, 0.2, 0.3],
                    TableField.METADATA.value: {"category": "test", "source": "unit_test"},
                },
                {
                    TableField.ID.value: "test2",
                    TableField.EMBEDDINGS.value: [0.4, 0.5, 0.6],
                    TableField.METADATA.value: {"category": "test", "source": "unit_test"},
                },
            ]
        )

        handler.insert("test_index", test_data)

        # Should call upsert on the index
        self.mock_index.upsert.assert_called_once()
        call_args = self.mock_index.upsert.call_args
        vectors = call_args[1]["vectors"]
        self.assertEqual(len(vectors), 2)
        self.assertIn("id", vectors[0])
        self.assertIn("values", vectors[0])
        self.assertIn("metadata", vectors[0])

    @patch("pinecone.Pinecone")
    def test_11_insert_with_string_embeddings(self, mock_pinecone_class):
        """Test inserting vectors with string-formatted embeddings."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test data with string embeddings (common from SQL queries)
        test_data = pd.DataFrame(
            [
                {
                    TableField.ID.value: "test1",
                    TableField.EMBEDDINGS.value: "[0.1, 0.2, 0.3]",  # String format
                    TableField.METADATA.value: {"category": "test"},
                }
            ]
        )

        handler.insert("test_index", test_data)

        # Should convert string to list and call upsert
        self.mock_index.upsert.assert_called_once()

    @patch("pinecone.Pinecone")
    def test_12_vector_similarity_search(self, mock_pinecone_class):
        """Test vector similarity search."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test vector search conditions
        conditions = [
            FilterCondition(column=TableField.SEARCH_VECTOR.value, op=FilterOperator.EQUAL, value=[0.1, 0.2, 0.3])
        ]

        result = handler.select("test_index", conditions=conditions, limit=5)

        self.assertIsInstance(result, pd.DataFrame)
        # Should execute similarity search query
        self.mock_index.query.assert_called_once()
        call_args = self.mock_index.query.call_args[1]
        self.assertEqual(call_args["vector"], [0.1, 0.2, 0.3])
        self.assertEqual(call_args["top_k"], 5)

    @patch("pinecone.Pinecone")
    def test_13_metadata_filtering(self, mock_pinecone_class):
        """Test filtering by metadata fields."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test metadata filtering with vector search
        conditions = [
            FilterCondition(column=TableField.SEARCH_VECTOR.value, op=FilterOperator.EQUAL, value=[0.1, 0.2, 0.3]),
            FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="documents"),
        ]

        result = handler.select("test_index", conditions=conditions)

        self.assertIsInstance(result, pd.DataFrame)
        # Should call query with metadata filter
        self.mock_index.query.assert_called_once()
        call_args = self.mock_index.query.call_args[1]
        self.assertIn("filter", call_args)
        self.assertIn("category", str(call_args["filter"]))

    @patch("pinecone.Pinecone")
    def test_14_id_based_retrieval(self, mock_pinecone_class):
        """Test retrieving vectors by ID."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test ID-based retrieval
        conditions = [FilterCondition(column=TableField.ID.value, op=FilterOperator.EQUAL, value="test_id")]

        result = handler.select("test_index", conditions=conditions)

        self.assertIsInstance(result, pd.DataFrame)
        # Should call query with ID filter
        self.mock_index.query.assert_called_once()
        call_args = self.mock_index.query.call_args[1]
        self.assertEqual(call_args["id"], "test_id")

    @patch("pinecone.Pinecone")
    def test_15_delete_by_id(self, mock_pinecone_class):
        """Test deleting vectors by ID."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        conditions = [
            FilterCondition(column=TableField.ID.value, op=FilterOperator.EQUAL, value=["test_id1", "test_id2"])
        ]

        handler.delete("test_index", conditions)

        # Should call delete with IDs
        self.mock_index.delete.assert_called_once()
        call_args = self.mock_index.delete.call_args[1]
        self.assertEqual(call_args["ids"], ["test_id1", "test_id2"])

    @patch("pinecone.Pinecone")
    def test_16_delete_by_metadata(self, mock_pinecone_class):
        """Test deleting vectors by metadata conditions."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        conditions = [FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="test_category")]

        handler.delete("test_index", conditions)

        # Should call delete with metadata filter
        self.mock_index.delete.assert_called_once()
        call_args = self.mock_index.delete.call_args[1]
        self.assertIn("filter", call_args)

    @patch("pinecone.Pinecone")
    def test_17_drop_table(self, mock_pinecone_class):
        """Test dropping an index."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        handler.drop_table("test_index", if_exists=True)

        # Should call delete_index
        self.mock_client.delete_index.assert_called_once_with("test_index")

    @patch("pinecone.Pinecone")
    def test_18_drop_table_not_exists(self, mock_pinecone_class):
        """Test dropping non-existent index."""
        from pinecone.core.openapi.shared.exceptions import NotFoundException

        mock_pinecone_class.return_value = self.mock_client
        self.mock_client.delete_index.side_effect = NotFoundException("Index not found")

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        # Should not raise exception when if_exists=True
        handler.drop_table("test_index", if_exists=True)

        self.mock_client.delete_index.assert_called_once()

    @patch("pinecone.Pinecone")
    def test_19_get_columns(self, mock_pinecone_class):
        """Test getting columns for an index."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)
        response = handler.get_columns("test_index")

        # Should return standard vector store columns
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)

    @patch("pinecone.Pinecone")
    def test_20_filter_operator_mapping(self, mock_pinecone_class):
        """Test different filter operators."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test different operators
        test_operators = [
            (FilterOperator.EQUAL, "$eq"),
            (FilterOperator.NOT_EQUAL, "$ne"),
            (FilterOperator.LESS_THAN, "$lt"),
            (FilterOperator.LESS_THAN_OR_EQUAL, "$lte"),
            (FilterOperator.GREATER_THAN, "$gt"),
            (FilterOperator.GREATER_THAN_OR_EQUAL, "$gte"),
            (FilterOperator.IN, "$in"),
            (FilterOperator.NOT_IN, "$nin"),
        ]

        for filter_op, expected_pinecone_op in test_operators:
            result = handler._get_pinecone_operator(filter_op)
            self.assertEqual(result, expected_pinecone_op)

    @patch("pinecone.Pinecone")
    def test_21_unsupported_filter_operator(self, mock_pinecone_class):
        """Test unsupported filter operator."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test unsupported operator
        with self.assertRaises(Exception) as context:
            handler._get_pinecone_operator(FilterOperator.LIKE)

        self.assertIn("not supported", str(context.exception))

    @patch("pinecone.Pinecone")
    def test_22_metadata_condition_translation(self, mock_pinecone_class):
        """Test metadata condition translation."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test single condition
        conditions = [FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="test")]

        result = handler._translate_metadata_condition(conditions)
        expected = {"category": {"$eq": "test"}}
        self.assertEqual(result, expected)

        # Test multiple conditions
        conditions = [
            FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="test"),
            FilterCondition(column="metadata.score", op=FilterOperator.GREATER_THAN, value=0.5),
        ]

        result = handler._translate_metadata_condition(conditions)
        self.assertIn("$and", result)
        self.assertEqual(len(result["$and"]), 2)

    @patch("pinecone.Pinecone")
    def test_23_error_handling_invalid_index(self, mock_pinecone_class):
        """Test error handling for invalid index operations."""
        mock_pinecone_class.return_value = self.mock_client
        # Mock index that doesn't exist
        self.mock_client.Index.return_value.describe_index_stats.side_effect = Exception("Index not found")

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test operations on non-existent index
        with self.assertRaises(Exception) as context:
            test_data = pd.DataFrame([{TableField.ID.value: "test1", TableField.EMBEDDINGS.value: [0.1, 0.2, 0.3]}])
            handler.insert("invalid_index", test_data)

        self.assertIn("are you sure the name is correct", str(context.exception))

    @patch("pinecone.Pinecone")
    def test_24_query_validation(self, mock_pinecone_class):
        """Test query validation requirements."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Test query without vector or ID should fail
        with self.assertRaises(Exception) as context:
            conditions = [FilterCondition(column="metadata.category", op=FilterOperator.EQUAL, value="test")]
            handler.select("test_index", conditions=conditions)

        self.assertIn("must provide either a search_vector or an ID", str(context.exception))

    @patch("pinecone.Pinecone")
    def test_25_batch_upsert_handling(self, mock_pinecone_class):
        """Test batch upsert for large datasets."""
        mock_pinecone_class.return_value = self.mock_client

        handler = PineconeHandler("test_pinecone", **self.handler_kwargs)

        # Create large dataset that should be batched
        large_data = []
        for i in range(150):  # More than UPSERT_BATCH_SIZE (99)
            large_data.append(
                {
                    TableField.ID.value: f"test{i}",
                    TableField.EMBEDDINGS.value: [0.1, 0.2, 0.3],
                    TableField.METADATA.value: {"index": i},
                }
            )

        test_data = pd.DataFrame(large_data)
        handler.insert("test_index", test_data)

        # Should make multiple upsert calls for batching
        self.assertGreater(self.mock_index.upsert.call_count, 1)

    def test_26_connection_args_validation(self):
        """Test connection arguments validation."""
        from mindsdb.integrations.handlers.pinecone_handler.connection_args import connection_args

        # Test required fields
        required_fields = ["api_key", "environment"]
        for field in required_fields:
            self.assertIn(field, connection_args)
            self.assertTrue(connection_args[field]["required"])

        # Test optional fields
        optional_fields = ["dimension", "metric", "pods", "replicas", "pod_type"]
        for field in optional_fields:
            self.assertIn(field, connection_args)
            self.assertFalse(connection_args[field]["required"])

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, "mock_client"):
            self.mock_client.reset_mock()
        if hasattr(self, "mock_index"):
            self.mock_index.reset_mock()


if __name__ == "__main__":
    unittest.main()
