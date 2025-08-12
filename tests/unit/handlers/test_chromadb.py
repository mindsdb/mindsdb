import unittest
import pytest
from collections import OrderedDict
from unittest.mock import patch, MagicMock
import pandas as pd

from base_handler_test import BaseHandlerTestSetup

try:
    from mindsdb.integrations.handlers.chromadb_handler.chromadb_handler import ChromaDBHandler
except ImportError:
    pytestmark = pytest.mark.skip("ChromaDB handler not installed")

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.integrations.libs.vectordatabase_handler import FilterCondition, FilterOperator


class TestChromaDBHandler(BaseHandlerTestSetup, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(host="localhost", port="8000", ssl=False)

    @property
    def dummy_connection_data_persistent(self):
        return OrderedDict(persist_directory="/tmp/chromadb_test")

    @property
    def dummy_connection_data_cloud(self):
        return OrderedDict(
            host="api.trychroma.com",
            port="8000",
            ssl=True,
            api_key="test_api_key",
            tenant="test_tenant",
            database="test_database",
        )

    def create_handler(self):
        return ChromaDBHandler("chromadb", connection_data=self.dummy_connection_data)

    def create_handler_persistent(self):
        return ChromaDBHandler("chromadb_persistent", connection_data=self.dummy_connection_data_persistent)

    def create_handler_cloud(self):
        return ChromaDBHandler("chromadb_cloud", connection_data=self.dummy_connection_data_cloud)

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")

    def test_connect_success_http_client(self):
        """
        Test if `connect` method successfully establishes a connection using HttpClient and sets `is_connected` flag to True.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Force disconnect first to ensure connect() is called
        self.handler.is_connected = False

        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)

    def test_check_connection_success(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.heartbeat.return_value = None
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_check_connection_failure(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on failure.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.heartbeat.side_effect = Exception("Heartbeat failed")
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Reset the handler connection state and client to force new client creation
        self.handler.is_connected = False
        self.handler._client = None

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    def test_create_table_success(self):
        """
        Test if the `create_table` method successfully creates a collection.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Create table
        self.handler.create_table("test_collection", if_not_exists=True)

        # Verify the collection was created
        mock_client.get_or_create_collection.assert_called_once()

    def test_insert_data_success(self):
        """
        Test if the `insert` method successfully inserts data into a collection.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Prepare test data
        test_data = pd.DataFrame(
            {
                "id": ["doc1", "doc2"],
                "content": ["First document", "Second document"],
                "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                "metadata": [{"type": "test"}, {"type": "sample"}],
            }
        )

        # Insert data
        response = self.handler.insert("test_collection", test_data)

        # Verify the response
        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.OK)
        self.assertEqual(response.affected_rows, 2)

        # Verify upsert was called
        mock_collection.upsert.assert_called_once()

    def test_select_data_success(self):
        """
        Test if the `select` method successfully retrieves data from a collection.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()

        # Mock the get response
        mock_collection.get.return_value = {
            "ids": ["doc1", "doc2"],
            "documents": ["First document", "Second document"],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
            "metadatas": [{"type": "test"}, {"type": "sample"}],
        }

        mock_client.get_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Select data
        result = self.handler.select("test_collection")

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertIn("id", result.columns)
        self.assertIn("content", result.columns)
        self.assertIn("embeddings", result.columns)
        self.assertIn("metadata", result.columns)

    def test_select_with_metadata_filter(self):
        """
        Test if the `select` method correctly applies metadata filters.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()

        # Mock the get response
        mock_collection.get.return_value = {
            "ids": ["doc1"],
            "documents": ["First document"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "metadatas": [{"type": "test"}],
        }

        mock_client.get_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Create filter condition
        conditions = [FilterCondition(column="metadata.type", op=FilterOperator.EQUAL, value="test")]

        # Select data with conditions
        result = self.handler.select("test_collection", conditions=conditions)

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)

        # Verify get was called with where filter
        mock_collection.get.assert_called_once()
        call_args = mock_collection.get.call_args
        self.assertIn("where", call_args[1])
        expected_where = {"type": {"$eq": "test"}}
        self.assertEqual(call_args[1]["where"], expected_where)

    def test_vector_similarity_search(self):
        """
        Test if the `select` method correctly performs vector similarity search.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()

        # Mock the query response
        mock_collection.query.return_value = {
            "ids": [["doc1", "doc2"]],
            "documents": [["First document", "Second document"]],
            "embeddings": [[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]],
            "metadatas": [[{"type": "test"}, {"type": "sample"}]],
            "distances": [[0.1, 0.3]],
        }

        mock_client.get_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Create vector filter condition
        conditions = [FilterCondition(column="embeddings", op=FilterOperator.EQUAL, value=[0.1, 0.2, 0.3])]

        # Perform similarity search
        result = self.handler.select("test_collection", conditions=conditions, limit=2)

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertIn("distance", result.columns)

        # Verify query was called
        mock_collection.query.assert_called_once()

    def test_delete_data_success(self):
        """
        Test if the `delete` method successfully deletes data from a collection.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()
        mock_client.get_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Create delete condition
        conditions = [FilterCondition(column="id", op=FilterOperator.EQUAL, value="doc1")]

        # Delete data
        self.handler.delete("test_collection", conditions=conditions)

        # Verify delete was called
        mock_collection.delete.assert_called_once()

    def test_get_tables_success(self):
        """
        Test if the `get_tables` method returns a list of collections.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()

        # Mock list_collections to return a list of Collection objects
        # ChromaDB returns Collection objects, not strings
        mock_collection1 = MagicMock()
        mock_collection1.name = "collection1"
        mock_collection2 = MagicMock()
        mock_collection2.name = "collection2"
        mock_client.list_collections.return_value = [mock_collection1, mock_collection2]
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Get tables
        response = self.handler.get_tables()

        # Verify the response
        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        assert isinstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)
        self.assertIn("table_name", response.data_frame.columns)

    def test_get_columns_collection_not_exists(self):
        """
        Test if the `get_columns` method returns an error when collection doesn't exist.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.get_collection.side_effect = ValueError("Collection not found")
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Get columns for non-existent table
        response = self.handler.get_columns("non_existent_collection")

        # Verify the error response
        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIn("does not exist", response.error_message)

    def test_drop_table_success(self):
        """
        Test if the `drop_table` method successfully drops a collection.
        """
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.delete_collection.return_value = None
        mock_chromadb.HttpClient.return_value = mock_client
        self.mock_connect.return_value = mock_chromadb

        # Set the client manually
        self.handler._client = mock_client

        # Drop table
        self.handler.drop_table("test_collection", if_exists=True)

        # Verify delete_collection was called
        mock_client.delete_collection.assert_called_once_with("test_collection")

    def test_connection_parameter_validation(self):
        """
        Test connection parameter validation.
        """
        # Test missing host and port
        with self.assertRaises(ValueError):
            invalid_data = {"host": "localhost"}  # Missing port
            ChromaDBHandler("test_invalid", connection_data=invalid_data)

        # Test conflicting host and persist_directory
        with self.assertRaises(ValueError):
            invalid_data = {"host": "localhost", "port": "8000", "persist_directory": "/tmp/test"}
            ChromaDBHandler("test_invalid", connection_data=invalid_data)

        # Test api_key without tenant/database
        with self.assertRaises(ValueError):
            invalid_data = {
                "host": "localhost",
                "port": "8000",
                "api_key": "test_key",  # Missing tenant and database
            }
            ChromaDBHandler("test_invalid", connection_data=invalid_data)

    def test_filter_operator_mapping(self):
        """
        Test if ChromaDB filter operators are correctly mapped.
        """
        # Test all supported operators
        operator_tests = [
            (FilterOperator.EQUAL, "$eq"),
            (FilterOperator.NOT_EQUAL, "$ne"),
            (FilterOperator.LESS_THAN, "$lt"),
            (FilterOperator.LESS_THAN_OR_EQUAL, "$lte"),
            (FilterOperator.GREATER_THAN, "$gt"),
            (FilterOperator.GREATER_THAN_OR_EQUAL, "$gte"),
            (FilterOperator.IN, "$in"),
            (FilterOperator.NOT_IN, "$nin"),
        ]

        for mindsdb_op, expected_chroma_op in operator_tests:
            result = self.handler._get_chromadb_operator(mindsdb_op)
            self.assertEqual(result, expected_chroma_op)

    def test_metadata_condition_translation(self):
        """
        Test if metadata conditions are correctly translated to ChromaDB format.
        """
        # Test single condition
        conditions = [FilterCondition(column="metadata.type", op=FilterOperator.EQUAL, value="test")]

        result = self.handler._translate_metadata_condition(conditions)
        expected = {"type": {"$eq": "test"}}
        self.assertEqual(result, expected)

        # Test multiple conditions
        conditions = [
            FilterCondition(column="metadata.type", op=FilterOperator.EQUAL, value="test"),
            FilterCondition(column="metadata.category", op=FilterOperator.IN, value=["A", "B"]),
        ]

        result = self.handler._translate_metadata_condition(conditions)
        expected = {"$and": [{"type": {"$eq": "test"}}, {"category": {"$in": ["A", "B"]}}]}
        self.assertEqual(result, expected)

    def test_document_id_processing(self):
        """
        Test document ID processing functionality.
        """
        # Test with existing IDs
        df_with_ids = pd.DataFrame({"id": [1, 2, 3], "content": ["doc1", "doc2", "doc3"]})
        result = self.handler._process_document_ids(df_with_ids)
        self.assertTrue(all(isinstance(id_val, str) for id_val in result["id"]))

        # Test without IDs (should generate hash-based IDs)
        df_without_ids = pd.DataFrame({"content": ["doc1", "doc2", "doc3"]})
        result = self.handler._process_document_ids(df_without_ids)
        self.assertIn("id", result.columns)
        self.assertEqual(len(result["id"].unique()), 3)  # All IDs should be unique


if __name__ == "__main__":
    unittest.main()
