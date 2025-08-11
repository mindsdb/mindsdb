import unittest
from unittest.mock import Mock, patch
import pandas as pd
import numpy as np
import tempfile
import os

from mindsdb.integrations.handlers.deeplake_handler.deeplake_handler import DeepLakeHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
)


class TestDeepLakeHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before each test method."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.test_connection_data = {
            "dataset_path": os.path.join(cls.temp_dir, "test_dataset"),
            "token": "test_token",
            "org_id": "test_org",
            "search_default_limit": 5,
            "search_distance_metric": "cosine",
            "search_exec_option": "python",
            "create_overwrite": True,
            "create_embedding_dim": 8,
            "create_max_chunk_size": 1000,
        }
        cls.handler_kwargs = {
            "connection_data": cls.test_connection_data,
        }

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the deeplake import to avoid dependency issues
        self.deeplake_mock = Mock()
        self.dataset_mock = Mock()

        # Setup mock dataset with required attributes
        self.dataset_mock.tensors = {"id": Mock(), "text": Mock(), "embedding": Mock(), "metadata": Mock()}
        self.dataset_mock.__len__ = Mock(return_value=3)

        # Setup mock tensor data access
        self.dataset_mock.text = [
            Mock(data=lambda: {"value": "Sample text 1"}),
            Mock(data=lambda: {"value": "Sample text 2"}),
            Mock(data=lambda: {"value": "Sample text 3"}),
        ]

        self.dataset_mock.embedding = [
            Mock(data=lambda: {"value": np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.float32)}),
            Mock(data=lambda: {"value": np.array([2, 3, 4, 5, 6, 7, 8, 9], dtype=np.float32)}),
            Mock(data=lambda: {"value": np.array([3, 4, 5, 6, 7, 8, 9, 10], dtype=np.float32)}),
        ]

        self.dataset_mock.id = [
            Mock(data=lambda: {"value": "id1"}),
            Mock(data=lambda: {"value": "id2"}),
            Mock(data=lambda: {"value": "id3"}),
        ]

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_01_handler_initialization(self, mock_logger):
        """Test handler initialization without actual Deep Lake connection."""
        with patch(
            "builtins.__import__", side_effect=lambda name, *args: self.deeplake_mock if name == "deeplake" else Mock()
        ):
            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            self.assertEqual(handler.name, "test_deeplake_handler")
            self.assertEqual(handler._search_limit, 5)
            self.assertEqual(handler._search_distance_metric, "cosine")
            self.assertEqual(handler._create_embedding_dim, 8)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_02_connection_success(self, mock_logger):
        """Test successful connection to Deep Lake."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            handler.connect()

            self.assertTrue(handler.is_connected)
            self.assertIsNotNone(handler.deeplake_client)
            self.assertIsNotNone(handler.dataset)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_03_connection_failure(self, mock_logger):
        """Test connection failure handling."""
        with patch("builtins.__import__", side_effect=ImportError("deeplake not found")):
            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            self.assertFalse(handler.is_connected)
            mock_logger.error.assert_called()

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_04_check_connection(self, mock_logger):
        """Test connection checking."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            response = handler.check_connection()

            self.assertTrue(response.success)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_05_get_tables_with_dataset(self, mock_logger):
        """Test getting tables when dataset exists."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            response = handler.get_tables()

            self.assertEqual(response.resp_type, RESPONSE_TYPE.TABLE)
            self.assertIn("test_dataset", response.data_frame["TABLE_NAME"].values)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_06_get_tables_without_dataset(self, mock_logger):
        """Test getting tables when no dataset exists."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.side_effect = Exception("Dataset not found")

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            handler.dataset = None
            response = handler.get_tables()

            self.assertEqual(response.resp_type, RESPONSE_TYPE.TABLE)
            self.assertEqual(len(response.data_frame), 0)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_07_create_table(self, mock_logger):
        """Test table/dataset creation."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            empty_dataset_mock = Mock()
            empty_dataset_mock.create_tensor = Mock()
            empty_dataset_mock.__enter__ = Mock(return_value=empty_dataset_mock)
            empty_dataset_mock.__exit__ = Mock(return_value=None)

            self.deeplake_mock.empty.return_value = empty_dataset_mock
            self.deeplake_mock.load.side_effect = Exception("Dataset not found")

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            handler.create_table("test_table")

            self.deeplake_mock.empty.assert_called_once()
            # Verify that tensors were created
            self.assertEqual(empty_dataset_mock.create_tensor.call_count, 4)  # text, embedding, metadata, id

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_08_insert_data(self, mock_logger):
        """Test data insertion."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock
            self.dataset_mock.append = Mock()
            self.dataset_mock.__enter__ = Mock(return_value=self.dataset_mock)
            self.dataset_mock.__exit__ = Mock(return_value=None)

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            # Prepare test data
            test_data = pd.DataFrame(
                [
                    {
                        TableField.ID.value: "test_id_1",
                        TableField.CONTENT.value: "Test content 1",
                        TableField.EMBEDDINGS.value: [1, 2, 3, 4, 5, 6, 7, 8],
                        TableField.METADATA.value: '{"category": "test"}',
                    },
                    {
                        TableField.ID.value: "test_id_2",
                        TableField.CONTENT.value: "Test content 2",
                        TableField.EMBEDDINGS.value: [2, 3, 4, 5, 6, 7, 8, 9],
                        TableField.METADATA.value: '{"category": "test"}',
                    },
                ]
            )

            handler.insert("test_table", test_data)

            # Verify that append was called for each row
            self.assertEqual(self.dataset_mock.append.call_count, 2)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_09_select_regular_query(self, mock_logger):
        """Test regular select query without vector search."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            result = handler.select("test_table", limit=2)

            self.assertIsInstance(result, pd.DataFrame)
            self.assertIn(TableField.ID.value, result.columns)
            self.assertIn(TableField.CONTENT.value, result.columns)
            self.assertIn(TableField.EMBEDDINGS.value, result.columns)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_10_select_vector_search(self, mock_logger):
        """Test vector similarity search."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            # Mock the embedding search method
            search_results = [
                {"index": 0, "score": 0.95},
                {"index": 1, "score": 0.87},
            ]
            self.dataset_mock.embedding.search = Mock(return_value=search_results)

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            conditions = [
                FilterCondition(
                    column=TableField.SEARCH_VECTOR.value,
                    op=FilterOperator.EQUAL,
                    value="[1, 2, 3, 4, 5, 6, 7, 8]",
                )
            ]

            result = handler.select("test_table", conditions=conditions)

            self.assertIsInstance(result, pd.DataFrame)
            self.assertIn(TableField.DISTANCE.value, result.columns)
            self.dataset_mock.embedding.search.assert_called_once()

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_11_delete_by_id(self, mock_logger):
        """Test deletion by ID."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            # Mock dataset deletion
            def mock_delete(index):
                return None

            self.dataset_mock.__delitem__ = Mock(side_effect=mock_delete)

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            conditions = [
                FilterCondition(
                    column=TableField.ID.value,
                    op=FilterOperator.EQUAL,
                    value="id1",
                )
            ]

            handler.delete("test_table", conditions)

            # Verify deletion was called
            self.dataset_mock.__delitem__.assert_called()

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_12_get_columns(self, mock_logger):
        """Test getting dataset columns/schema."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            response = handler.get_columns("test_table")

            self.assertEqual(response.resp_type, RESPONSE_TYPE.TABLE)
            self.assertIn("COLUMN_NAME", response.data_frame.columns)
            self.assertIn("DATA_TYPE", response.data_frame.columns)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_13_translate_conditions(self, mock_logger):
        """Test filter condition translation."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            conditions = [
                FilterCondition(
                    column="metadata.category",
                    op=FilterOperator.EQUAL,
                    value="test",
                ),
                FilterCondition(
                    column="metadata.score",
                    op=FilterOperator.GREATER_THAN,
                    value=0.5,
                ),
            ]

            result = handler._translate_conditions(conditions)

            self.assertIsInstance(result, dict)
            self.assertIn("category", result)
            self.assertIn("score", result)
            self.assertEqual(result["category"], "test")
            self.assertEqual(result["score"], {"$gt": 0.5})

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_14_error_handling_no_dataset(self, mock_logger):
        """Test error handling when dataset doesn't exist."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.side_effect = Exception("Dataset not found")

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            handler.dataset = None

            with self.assertRaises(Exception) as context:
                handler.select("nonexistent_table")

            self.assertIn("does not exist", str(context.exception))

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_15_error_handling_delete_without_conditions(self, mock_logger):
        """Test error handling for delete without conditions."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)

            with self.assertRaises(Exception) as context:
                handler.delete("test_table", None)

            self.assertIn("conditions are required", str(context.exception))

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_16_disconnect(self, mock_logger):
        """Test disconnection."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            handler.disconnect()

            self.assertFalse(handler.is_connected)
            self.assertIsNone(handler.dataset)

    @patch("mindsdb.integrations.handlers.deeplake_handler.deeplake_handler.logger")
    def test_17_drop_table_warning(self, mock_logger):
        """Test drop table operation (should log warning)."""
        with patch("builtins.__import__", return_value=self.deeplake_mock):
            self.deeplake_mock.load.return_value = self.dataset_mock

            handler = DeepLakeHandler("test_deeplake_handler", **self.handler_kwargs)
            handler.drop_table("test_table")

            # Should log a warning about dataset deletion
            mock_logger.warning.assert_called()

    def tearDown(self):
        """Clean up after each test."""
        # Clean up any created files/directories if needed
        pass

    @classmethod
    def tearDownClass(cls):
        """Clean up class-level fixtures."""
        # Clean up temporary directory
        import shutil

        if os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)


if __name__ == "__main__":
    unittest.main()
