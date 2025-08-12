import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from collections import OrderedDict

from mindsdb.integrations.handlers.chromadb_handler.chromadb_handler import ChromaDBHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import FilterCondition, FilterOperator


class TestChromaDBHandler(unittest.TestCase):
    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_0_check_connection_remote(self, mock_get_chromadb):
        """Test connection check for remote ChromaDB instance."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.heartbeat.return_value = None
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)
        response = handler.check_connection()
        assert response.success is True

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_1_check_connection_persistent(self, mock_get_chromadb):
        """Test connection check for persistent ChromaDB instance."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.heartbeat.return_value = None
        mock_chromadb.PersistentClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(persist_directory="/tmp/chromadb_test")

        handler = ChromaDBHandler("test_chromadb_persistent", connection_data=connection_data)
        response = handler.check_connection()
        assert response.success is True

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_2_create_table(self, mock_get_chromadb):
        """Test table (collection) creation."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Create table
        handler.create_table("test_collection", if_not_exists=True)

        # Verify the collection was created
        mock_client.get_or_create_collection.assert_called_once()

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_3_insert_data(self, mock_get_chromadb):
        """Test data insertion."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

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
        response = handler.insert("test_collection", test_data)

        # Verify the response
        assert response.type == RESPONSE_TYPE.OK
        assert response.affected_rows == 2

        # Verify upsert was called
        mock_collection.upsert.assert_called_once()

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_4_select_data(self, mock_get_chromadb):
        """Test data selection."""
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
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Select data
        result = handler.select("test_collection")

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "id" in result.columns
        assert "content" in result.columns
        assert "embeddings" in result.columns
        assert "metadata" in result.columns

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_5_select_with_conditions(self, mock_get_chromadb):
        """Test data selection with filter conditions."""
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
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Create filter condition
        conditions = [FilterCondition(column="metadata.type", op=FilterOperator.EQUAL, value="test")]

        # Select data with conditions
        result = handler.select("test_collection", conditions=conditions)

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

        # Verify get was called with where filter
        mock_collection.get.assert_called_once()
        call_args = mock_collection.get.call_args
        assert "where" in call_args[1]

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_6_vector_similarity_search(self, mock_get_chromadb):
        """Test vector similarity search."""
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
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Create vector filter condition
        conditions = [FilterCondition(column="embeddings", op=FilterOperator.EQUAL, value=[0.1, 0.2, 0.3])]

        # Perform similarity search
        result = handler.select("test_collection", conditions=conditions, limit=2)

        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "distance" in result.columns

        # Verify query was called
        mock_collection.query.assert_called_once()

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_7_delete_data(self, mock_get_chromadb):
        """Test data deletion."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_collection = MagicMock()
        mock_client.get_collection.return_value = mock_collection
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Create delete condition
        conditions = [FilterCondition(column="id", op=FilterOperator.EQUAL, value="doc1")]

        # Delete data
        handler.delete("test_collection", conditions=conditions)

        # Verify delete was called
        mock_collection.delete.assert_called_once()

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_8_get_tables(self, mock_get_chromadb):
        """Test getting list of tables (collections)."""
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
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Get tables
        response = handler.get_tables()

        # Verify the response
        assert response.type == RESPONSE_TYPE.TABLE
        assert isinstance(response.data_frame, pd.DataFrame)
        assert len(response.data_frame) == 2
        assert "table_name" in response.data_frame.columns

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_9_get_columns_error_case(self, mock_get_chromadb):
        """Test getting column information for a non-existent table."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.get_collection.side_effect = ValueError("Collection not found")
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Get columns for non-existent table
        response = handler.get_columns("non_existent_collection")

        # Verify the error response
        assert response.type == RESPONSE_TYPE.ERROR
        assert "does not exist" in response.error_message

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_10_drop_table(self, mock_get_chromadb):
        """Test dropping a table (collection)."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.delete_collection.return_value = None
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Connect first
        handler.connect()

        # Drop table
        handler.drop_table("test_collection", if_exists=True)

        # Verify delete_collection was called
        mock_client.delete_collection.assert_called_once_with("test_collection")

    def test_11_connection_validation(self):
        """Test connection parameter validation."""
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

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_12_connection_failure(self, mock_get_chromadb):
        """Test connection failure handling."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_client.heartbeat.side_effect = Exception("Connection failed")
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)
        response = handler.check_connection()
        assert response.success is False
        assert response.error_message is not None

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_13_dataframe_metadata_conversion(self, mock_get_chromadb):
        """Test metadata conversion from DataFrame format."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Test dictionary metadata
        metadata_dict = {"key": "value", "number": 123}
        result = handler._dataframe_metadata_to_chroma_metadata(metadata_dict)
        assert result == metadata_dict

        # Test string metadata
        metadata_str = "{'key': 'value', 'number': 123}"
        result = handler._dataframe_metadata_to_chroma_metadata(metadata_str)
        assert result == {"key": "value", "number": 123}

        # Test None metadata
        result = handler._dataframe_metadata_to_chroma_metadata(None)
        assert result is None

        # Test empty dict
        result = handler._dataframe_metadata_to_chroma_metadata({})
        assert result is None

    @patch("mindsdb.integrations.handlers.chromadb_handler.chromadb_handler.get_chromadb")
    def test_14_process_document_ids(self, mock_get_chromadb):
        """Test document ID processing."""
        mock_chromadb = MagicMock()
        mock_client = MagicMock()
        mock_chromadb.HttpClient.return_value = mock_client
        mock_get_chromadb.return_value = mock_chromadb

        connection_data = OrderedDict(host="localhost", port="8000", ssl=False)

        handler = ChromaDBHandler("test_chromadb_remote", connection_data=connection_data)

        # Test with existing IDs
        df_with_ids = pd.DataFrame({"id": [1, 2, 3], "content": ["doc1", "doc2", "doc3"]})
        result = handler._process_document_ids(df_with_ids)
        assert all(isinstance(id_val, str) for id_val in result["id"])

        # Test without IDs (should generate hash-based IDs)
        df_without_ids = pd.DataFrame({"content": ["doc1", "doc2", "doc3"]})
        result = handler._process_document_ids(df_without_ids)
        assert "id" in result.columns
        assert len(result["id"].unique()) == 3  # All IDs should be unique


if __name__ == "__main__":
    unittest.main()
