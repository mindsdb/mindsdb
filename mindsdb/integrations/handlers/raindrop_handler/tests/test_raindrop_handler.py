import unittest
from unittest.mock import Mock, patch
import pandas as pd

from mindsdb.integrations.handlers.raindrop_handler.raindrop_handler import RaindropHandler, RaindropAPIClient
from mindsdb.integrations.handlers.raindrop_handler.raindrop_tables import (
    RaindropsTable,
    CollectionsTable,
    TagsTable,
    ParseTable,
)


class TestRaindropHandler(unittest.TestCase):
    """Test cases for RaindropHandler"""

    def setUp(self):
        self.handler = RaindropHandler("test_raindrop_handler")
        self.handler.connection_data = {"api_key": "test_api_key"}

    def test_init(self):
        """Test handler initialization"""
        self.assertEqual(self.handler.name, "test_raindrop_handler")
        self.assertFalse(self.handler.is_connected)
        self.assertIn("raindrops", self.handler._tables)
        self.assertIn("bookmarks", self.handler._tables)
        self.assertIn("collections", self.handler._tables)

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient")
    def test_connect(self, mock_client):
        """Test connection establishment"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance

        result = self.handler.connect()

        mock_client.assert_called_once_with("test_api_key")
        self.assertEqual(result, mock_instance)
        self.assertTrue(self.handler.is_connected)

    def test_connect_missing_api_key(self):
        """Test connection with missing API key"""
        self.handler.connection_data = {}

        with self.assertRaises(ValueError) as context:
            self.handler.connect()

        self.assertIn("API key is required", str(context.exception))

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient")
    def test_check_connection_success(self, mock_client):
        """Test successful connection check"""
        mock_instance = Mock()
        mock_instance.get_user_stats.return_value = {"result": True}
        mock_client.return_value = mock_instance

        result = self.handler.check_connection()

        self.assertTrue(result.success)
        self.assertTrue(self.handler.is_connected)

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient")
    def test_check_connection_failure(self, mock_client):
        """Test failed connection check"""
        mock_instance = Mock()
        mock_instance.get_user_stats.return_value = {"result": False}
        mock_client.return_value = mock_instance

        result = self.handler.check_connection()

        self.assertFalse(result.success)
        self.assertFalse(self.handler.is_connected)

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient")
    def test_check_connection_exception(self, mock_client):
        """Test connection check with exception"""
        mock_instance = Mock()
        mock_instance.get_user_stats.side_effect = Exception("Connection error")
        mock_client.return_value = mock_instance

        result = self.handler.check_connection()

        self.assertFalse(result.success)
        self.assertIn("Connection error", result.error_message)


class TestRaindropAPIClient(unittest.TestCase):
    """Test cases for RaindropAPIClient"""

    def setUp(self):
        self.client = RaindropAPIClient("test_api_key")

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.requests")
    def test_make_request_get(self, mock_requests):
        """Test GET request"""
        mock_response = Mock()
        mock_response.json.return_value = {"result": True, "items": []}
        mock_response.raise_for_status.return_value = None
        mock_requests.request.return_value = mock_response

        result = self.client._make_request("GET", "/user/stats")

        mock_requests.request.assert_called_once_with(
            method="GET",
            url="https://api.raindrop.io/rest/v1/user/stats",
            headers={"Authorization": "Bearer test_api_key", "Content-Type": "application/json"},
            params=None,
            json=None,
        )
        self.assertEqual(result, {"result": True, "items": []})

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.requests.request")
    def test_rate_limiting(self, mock_request):
        """Test that rate limiting works correctly"""
        import time

        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"result": True, "items": []}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        # Reset request times to ensure clean state
        self.client.request_times = []

        # Make multiple rapid requests
        start_time = time.time()
        for i in range(3):
            self.client._make_request("GET", "/user/stats")
        end_time = time.time()

        # Should take at least 1 second due to rate limiting (2 requests/second limit)
        total_time = end_time - start_time
        self.assertGreaterEqual(total_time, 1.0, "Rate limiting should add delays between requests")

        # Should have tracked the requests (rate limiter may clean up old entries)
        self.assertGreaterEqual(len(self.client.request_times), 1, "Should track at least the most recent request")

    @patch.object(RaindropAPIClient, "_make_request")
    def test_get_raindrops_optimized_pagination(self, mock_request):
        """Test that get_raindrops optimizes page sizes based on LIMIT"""
        # Mock response with items
        mock_response = {"result": True, "items": [{"_id": 1, "title": "Test"}] * 5, "count": 5}
        mock_request.return_value = mock_response

        # Test small LIMIT - should use smaller page size
        result = self.client.get_raindrops(max_results=5)
        self.assertEqual(len(result["items"]), 5)

        # Verify the request was made with optimized page size
        args, kwargs = mock_request.call_args
        self.assertEqual(kwargs["params"]["perpage"], 5, "Should use small page size for small LIMIT")

        # Reset mock
        mock_request.reset_mock()

        # Test larger LIMIT - should use larger page size
        result = self.client.get_raindrops(max_results=100)
        args, kwargs = mock_request.call_args
        self.assertEqual(kwargs["params"]["perpage"], 50, "Should use larger page size for bigger LIMIT")

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.requests")
    def test_make_request_post(self, mock_requests):
        """Test POST request with data"""
        mock_response = Mock()
        mock_response.json.return_value = {"result": True, "item": {}}
        mock_response.raise_for_status.return_value = None
        mock_requests.request.return_value = mock_response

        test_data = {"title": "Test"}
        self.client._make_request("POST", "/raindrop", data=test_data)

        mock_requests.request.assert_called_once_with(
            method="POST",
            url="https://api.raindrop.io/rest/v1/raindrop",
            headers={"Authorization": "Bearer test_api_key", "Content-Type": "application/json"},
            params=None,
            json=test_data,
        )

    @patch.object(RaindropAPIClient, "_make_request")
    def test_get_raindrops(self, mock_request):
        """Test get_raindrops method"""
        mock_request.return_value = {"result": True, "items": []}

        self.client.get_raindrops(collection_id=123, search="test", page=1)

        mock_request.assert_called_once_with(
            "GET", "/raindrops/123", params={"page": 1, "perpage": 50, "search": "test"}
        )

    @patch.object(RaindropAPIClient, "_make_request")
    def test_create_raindrop(self, mock_request):
        """Test create_raindrop method"""
        mock_request.return_value = {"result": True, "item": {}}

        raindrop_data = {"link": "https://example.com", "title": "Test"}
        self.client.create_raindrop(raindrop_data)

        mock_request.assert_called_once_with("POST", "/raindrop", data=raindrop_data)

    @patch.object(RaindropAPIClient, "_make_request")
    def test_update_raindrop(self, mock_request):
        """Test update_raindrop method"""
        mock_request.return_value = {"result": True, "item": {}}

        raindrop_data = {"title": "Updated Title"}
        self.client.update_raindrop(123, raindrop_data)

        mock_request.assert_called_once_with("PUT", "/raindrop/123", data=raindrop_data)

    @patch.object(RaindropAPIClient, "_make_request")
    def test_delete_raindrop(self, mock_request):
        """Test delete_raindrop method"""
        mock_request.return_value = {"result": True}

        self.client.delete_raindrop(123)

        mock_request.assert_called_once_with("DELETE", "/raindrop/123")

    def test_make_request_invalid_endpoint(self):
        """Test that invalid endpoints are rejected"""
        with self.assertRaises(ValueError) as context:
            self.client._make_request("GET", "/invalid/endpoint")

        self.assertIn("Invalid endpoint", str(context.exception))
        self.assertIn("Only Raindrop.io API endpoints are allowed", str(context.exception))

    def test_make_request_path_traversal_attempt(self):
        """Test that path traversal attempts are rejected"""
        with self.assertRaises(ValueError) as context:
            self.client._make_request("GET", "../../../etc/passwd")

        self.assertIn("Invalid endpoint", str(context.exception))


class TestRaindropsTable(unittest.TestCase):
    """Test cases for RaindropsTable"""

    def setUp(self):
        self.handler = Mock()
        self.handler.connection = Mock()
        self.table = RaindropsTable(self.handler)

    def test_apply_local_filters_greater_than(self):
        """Test _apply_local_filters with greater than operator"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "created": "2024-01-01T00:00:00Z", "sort": 10},
                {"_id": 2, "created": "2024-01-15T00:00:00Z", "sort": 20},
                {"_id": 3, "created": "2024-01-30T00:00:00Z", "sort": 30},
            ]
        )

        # Test date comparison
        conditions = [[">", "created", "2024-01-15T00:00:00Z"]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["_id"].iloc[0], 3)

        # Test numeric comparison
        conditions = [[">", "sort", 15]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["_id"].tolist(), [2, 3])

    def test_apply_local_filters_less_than_equal(self):
        """Test _apply_local_filters with less than or equal operator"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "sort": 10},
                {"_id": 2, "sort": 20},
                {"_id": 3, "sort": 30},
            ]
        )

        conditions = [["<=", "sort", 20]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["_id"].tolist(), [1, 2])

    def test_apply_local_filters_between(self):
        """Test _apply_local_filters with BETWEEN operator"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "created": "2024-01-01T00:00:00Z"},
                {"_id": 2, "created": "2024-01-15T00:00:00Z"},
                {"_id": 3, "created": "2024-01-30T00:00:00Z"},
            ]
        )

        conditions = [["between", "created", ("2024-01-05T00:00:00Z", "2024-01-25T00:00:00Z")]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["_id"].iloc[0], 2)

    def test_apply_local_filters_like(self):
        """Test _apply_local_filters with LIKE operator"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "title": "Python Tutorial"},
                {"_id": 2, "title": "JavaScript Guide"},
                {"_id": 3, "title": "Python Best Practices"},
            ]
        )

        conditions = [["like", "title", "%Python%"]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["_id"].tolist(), [1, 3])

    def test_apply_local_filters_in(self):
        """Test _apply_local_filters with IN operator"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "tags": "python,javascript"},
                {"_id": 2, "tags": "java,ruby"},
                {"_id": 3, "tags": "python,django"},
            ]
        )

        conditions = [["in", "_id", [1, 3]]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["_id"].tolist(), [1, 3])

    def test_apply_local_filters_important_flag(self):
        """Test _apply_local_filters with important flag"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "important": True},
                {"_id": 2, "important": False},
                {"_id": 3, "important": True},
            ]
        )

        conditions = [["=", "important", True]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["_id"].tolist(), [1, 3])

    def test_apply_local_filters_multiple_conditions(self):
        """Test _apply_local_filters with multiple conditions"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "important": True, "sort": 10},
                {"_id": 2, "important": False, "sort": 20},
                {"_id": 3, "important": True, "sort": 30},
            ]
        )

        conditions = [["=", "important", True], [">", "sort", 15]]
        result = self.table._apply_local_filters(test_data.copy(), conditions)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["_id"].iloc[0], 3)

    def test_apply_local_filters_unsupported_operator(self):
        """Test _apply_local_filters with unsupported operator"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "title": "Test"},
            ]
        )

        conditions = [["regex", "title", ".*"]]
        with self.assertLogs(level="WARNING") as log:
            result = self.table._apply_local_filters(test_data.copy(), conditions)
            self.assertIn("Unsupported operator 'regex'", log.output[0])
        self.assertEqual(len(result), 1)  # Original data should be returned

    def test_apply_local_filters_missing_column(self):
        """Test _apply_local_filters with missing column"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "title": "Test"},
            ]
        )

        conditions = [["=", "missing_column", "value"]]
        with self.assertLogs(level="WARNING") as log:
            result = self.table._apply_local_filters(test_data.copy(), conditions)
            self.assertIn("Column 'missing_column' not found", log.output[0])
        self.assertEqual(len(result), 1)  # Original data should be returned

    def test_apply_ordering(self):
        """Test _apply_ordering method"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "sort": 30, "title": "Z Title"},
                {"_id": 2, "sort": 10, "title": "A Title"},
                {"_id": 3, "sort": 20, "title": "B Title"},
            ]
        )

        # Mock order by conditions
        order_conditions = [
            type("MockOrder", (), {"column": "sort", "ascending": True})(),
        ]

        result = self.table._apply_ordering(test_data.copy(), order_conditions)
        self.assertEqual(result["_id"].tolist(), [2, 3, 1])  # Sorted by sort ascending

    def test_apply_ordering_descending(self):
        """Test _apply_ordering method with descending order"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "sort": 10},
                {"_id": 2, "sort": 30},
                {"_id": 3, "sort": 20},
            ]
        )

        # Mock order by conditions
        order_conditions = [
            type("MockOrder", (), {"column": "sort", "ascending": False})(),
        ]

        result = self.table._apply_ordering(test_data.copy(), order_conditions)
        self.assertEqual(result["_id"].tolist(), [2, 3, 1])  # Sorted by sort descending

    def test_apply_ordering_multiple_columns(self):
        """Test _apply_ordering method with multiple columns"""
        test_data = pd.DataFrame(
            [
                {"_id": 1, "sort": 10, "title": "B"},
                {"_id": 2, "sort": 20, "title": "A"},
                {"_id": 3, "sort": 10, "title": "A"},
            ]
        )

        # Mock order by conditions
        order_conditions = [
            type("MockOrder", (), {"column": "sort", "ascending": True})(),
            type("MockOrder", (), {"column": "title", "ascending": True})(),
        ]

        result = self.table._apply_ordering(test_data.copy(), order_conditions)
        self.assertEqual(result["_id"].tolist(), [3, 1, 2])  # Sort by sort then title

    def test_get_columns(self):
        """Test get_columns method"""
        columns = self.table.get_columns()

        expected_columns = [
            "_id",
            "link",
            "title",
            "excerpt",
            "note",
            "type",
            "cover",
            "tags",
            "important",
            "reminder",
            "removed",
            "created",
            "lastUpdate",
            "domain",
            "collection.id",
            "collection.title",
            "user.id",
            "broken",
            "cache",
            "file.name",
            "file.size",
            "file.type",
        ]

        self.assertEqual(columns, expected_columns)

    def test_normalize_raindrop_data(self):
        """Test _normalize_raindrop_data method"""
        test_data = pd.DataFrame(
            [
                {
                    "_id": 123,
                    "title": "Test",
                    "collection": {"$id": 456, "title": "Test Collection"},
                    "user": {"$id": 789},
                    "file": {"name": "test.pdf", "size": 1024, "type": "pdf"},
                    "tags": ["tag1", "tag2"],
                    "created": "2024-01-01T00:00:00Z",
                    "lastUpdate": "2024-01-02T00:00:00Z",
                }
            ]
        )

        result = self.table._normalize_raindrop_data(test_data)

        self.assertEqual(result["collection.id"].iloc[0], 456)
        self.assertEqual(result["collection.title"].iloc[0], "Test Collection")
        self.assertEqual(result["user.id"].iloc[0], 789)
        self.assertEqual(result["file.name"].iloc[0], "test.pdf")
        self.assertEqual(result["file.size"].iloc[0], 1024)
        self.assertEqual(result["file.type"].iloc[0], "pdf")
        self.assertEqual(result["tags"].iloc[0], "tag1,tag2")

    def test_prepare_raindrop_data(self):
        """Test _prepare_raindrop_data method"""
        input_data = {
            "link": "https://example.com",
            "title": "Test",
            "collection_id": 123,
            "tags": "tag1,tag2",
            "important": True,
        }

        result = self.table._prepare_raindrop_data(input_data)

        expected = {
            "link": "https://example.com",
            "title": "Test",
            "collection": {"$id": 123},
            "tags": ["tag1", "tag2"],
            "important": True,
        }

        self.assertEqual(result, expected)

    def test_prepare_raindrop_data_with_list_tags(self):
        """Test _prepare_raindrop_data method with list tags"""
        input_data = {"link": "https://example.com", "tags": ["tag1", "tag2"]}

        result = self.table._prepare_raindrop_data(input_data)

        self.assertEqual(result["tags"], ["tag1", "tag2"])

    def test_normalize_raindrop_data_missing_columns(self):
        """Test _normalize_raindrop_data method with missing columns"""
        # Test with minimal data that might come from API (missing some nested fields)
        test_data = pd.DataFrame(
            [
                {
                    "_id": 123,
                    "title": "Test Bookmark",
                    "link": "https://example.com",
                    "created": "2024-01-01T00:00:00Z",
                    # Note: missing collection, user, file, tags fields
                }
            ]
        )

        result = self.table._normalize_raindrop_data(test_data)

        # Check that all expected columns exist
        expected_columns = self.table.get_columns()
        for col in expected_columns:
            self.assertIn(col, result.columns, f"Missing column: {col}")

        # Check that missing nested fields are handled gracefully
        self.assertIsNone(result["collection.id"].iloc[0])
        self.assertIsNone(result["collection.title"].iloc[0])
        self.assertIsNone(result["user.id"].iloc[0])
        self.assertIsNone(result["file.name"].iloc[0])
        self.assertIsNone(result["tags"].iloc[0])

    def test_normalize_raindrop_data_empty_dataframe(self):
        """Test _normalize_raindrop_data method with empty DataFrame"""
        empty_df = pd.DataFrame()

        result = self.table._normalize_raindrop_data(empty_df)

        # Should return the same empty DataFrame
        self.assertTrue(result.empty)

    def test_select_with_empty_data(self):
        """Test select method with empty data from API"""
        # Mock empty response from API
        self.handler.connection.get_raindrops.return_value = {"result": True, "items": []}

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = ([], [], [], None)
            mock_parser.return_value = mock_parser_instance

            # Mock executor to return DataFrame with columns (as it should after our fix)
            mock_executor_instance = Mock()
            empty_df_with_columns = pd.DataFrame(columns=self.table.get_columns())
            mock_executor_instance.execute_query.return_value = empty_df_with_columns
            mock_executor.return_value = mock_executor_instance

            query = Mock()
            result = self.table.select(query)

            # Should return DataFrame with all expected columns
            expected_columns = self.table.get_columns()
            for col in expected_columns:
                self.assertIn(col, result.columns, f"Missing column in empty result: {col}")

            # Should be empty but have all columns
            self.assertTrue(result.empty)
            self.assertEqual(len(result.columns), len(expected_columns))

    def test_select_optimized_for_limit(self):
        """Test that SELECT with LIMIT uses optimized pagination"""
        # Mock empty response from API (no items)
        self.handler.connection.get_raindrops.return_value = {"result": True, "items": []}

        # Mock the SELECT query components with LIMIT 3
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = ([], [], [], 3)  # LIMIT 3
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            empty_df_with_columns = pd.DataFrame(columns=self.table.get_columns())
            mock_executor_instance.execute_query.return_value = empty_df_with_columns
            mock_executor.return_value = mock_executor_instance

            query = Mock()
            self.table.select(query)

            # Verify that get_raindrops was called with max_results=3 for optimization
            self.handler.connection.get_raindrops.assert_called_once()
            args, kwargs = self.handler.connection.get_raindrops.call_args
            self.assertEqual(kwargs.get("max_results"), 3, "Should pass LIMIT to API for optimization")

    def test_normalize_raindrop_data_partial_nested_data(self):
        """Test _normalize_raindrop_data with partial nested data"""
        test_data = pd.DataFrame(
            [
                {
                    "_id": 123,
                    "title": "Test",
                    "collection": {"$id": 456},  # Missing title
                    "user": None,  # Explicitly None
                    "tags": [],  # Empty list
                    "created": "2024-01-01T00:00:00Z",
                }
            ]
        )

        result = self.table._normalize_raindrop_data(test_data)

        # Check that partial data is handled correctly
        self.assertEqual(result["collection.id"].iloc[0], 456)
        self.assertIsNone(result["collection.title"].iloc[0])  # Missing field
        self.assertIsNone(result["user.id"].iloc[0])  # None user
        self.assertEqual(result["tags"].iloc[0], "")  # Empty list becomes empty string

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser")
    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor")
    def test_select_basic(self, mock_executor, mock_parser):
        """Test basic select operation"""
        # Mock parser
        mock_parser_instance = Mock()
        mock_parser_instance.parse_query.return_value = ([], [], [], None)
        mock_parser.return_value = mock_parser_instance

        # Mock executor
        mock_executor_instance = Mock()
        mock_executor_instance.execute_query.return_value = pd.DataFrame()
        mock_executor.return_value = mock_executor_instance

        # Mock API response
        self.handler.connection.get_raindrops.return_value = {"result": True, "items": [{"_id": 123, "title": "Test"}]}

        query = Mock()
        result = self.table.select(query)

        self.assertIsInstance(result, pd.DataFrame)

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.INSERTQueryParser")
    def test_insert_single(self, mock_parser):
        """Test single insert operation"""
        mock_parser_instance = Mock()
        mock_parser_instance.parse_query.return_value = {"link": "https://example.com", "title": "Test"}
        mock_parser.return_value = mock_parser_instance

        self.handler.connection.create_raindrop.return_value = {"result": True}

        query = Mock()
        self.table.insert(query)

        self.handler.connection.create_raindrop.assert_called_once()

    @patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.INSERTQueryParser")
    def test_insert_multiple(self, mock_parser):
        """Test multiple insert operation"""
        mock_parser_instance = Mock()
        mock_parser_instance.parse_query.return_value = [
            {"link": "https://example1.com", "title": "Test1"},
            {"link": "https://example2.com", "title": "Test2"},
        ]
        mock_parser.return_value = mock_parser_instance

        self.handler.connection.create_multiple_raindrops.return_value = {"result": True}

        query = Mock()
        self.table.insert(query)

        self.handler.connection.create_multiple_raindrops.assert_called_once()


class TestCollectionsTable(unittest.TestCase):
    """Test cases for CollectionsTable"""

    def setUp(self):
        self.handler = Mock()
        self.handler.connection = Mock()
        self.table = CollectionsTable(self.handler)

    def test_get_columns(self):
        """Test get_columns method"""
        columns = self.table.get_columns()

        expected_columns = [
            "_id",
            "title",
            "description",
            "color",
            "view",
            "public",
            "sort",
            "count",
            "created",
            "lastUpdate",
            "expanded",
            "parent.id",
            "user.id",
            "cover",
            "access.level",
            "access.draggable",
        ]

        self.assertEqual(columns, expected_columns)

    def test_normalize_collection_data(self):
        """Test _normalize_collection_data method"""
        test_data = pd.DataFrame(
            [
                {
                    "_id": 123,
                    "title": "Test Collection",
                    "parent": {"$id": 456},
                    "user": {"$id": 789},
                    "access": {"level": 4, "draggable": True},
                    "cover": ["https://example.com/cover.jpg"],
                    "created": "2024-01-01T00:00:00Z",
                    "lastUpdate": "2024-01-02T00:00:00Z",
                }
            ]
        )

        result = self.table._normalize_collection_data(test_data)

        self.assertEqual(result["parent.id"].iloc[0], 456)
        self.assertEqual(result["user.id"].iloc[0], 789)
        self.assertEqual(result["access.level"].iloc[0], 4)
        self.assertEqual(result["access.draggable"].iloc[0], True)
        self.assertEqual(result["cover"].iloc[0], "https://example.com/cover.jpg")

    def test_normalize_collection_data_missing_columns(self):
        """Test _normalize_collection_data method with missing columns"""
        # Test with minimal data that might come from API (missing some nested fields)
        test_data = pd.DataFrame(
            [
                {
                    "_id": 123,
                    "title": "Test Collection",
                    "created": "2024-01-01T00:00:00Z",
                    # Note: missing parent, user, access, cover fields
                }
            ]
        )

        result = self.table._normalize_collection_data(test_data)

        # Check that all expected columns exist
        expected_columns = self.table.get_columns()
        for col in expected_columns:
            self.assertIn(col, result.columns, f"Missing column: {col}")

        # Check that missing nested fields are handled gracefully
        self.assertIsNone(result["parent.id"].iloc[0])
        self.assertIsNone(result["user.id"].iloc[0])
        self.assertIsNone(result["access.level"].iloc[0])
        self.assertIsNone(result["access.draggable"].iloc[0])
        self.assertIsNone(result["cover"].iloc[0])

    def test_normalize_collection_data_empty_dataframe(self):
        """Test _normalize_collection_data method with empty DataFrame"""
        empty_df = pd.DataFrame()

        result = self.table._normalize_collection_data(empty_df)

        # Should return the same empty DataFrame
        self.assertTrue(result.empty)

    def test_normalize_collection_data_partial_nested_data(self):
        """Test _normalize_collection_data with partial nested data"""
        test_data = pd.DataFrame(
            [
                {
                    "_id": 123,
                    "title": "Test Collection",
                    "parent": {"$id": 456},  # Missing other parent fields
                    "user": None,  # Explicitly None
                    "access": {"level": 4},  # Missing draggable
                    "created": "2024-01-01T00:00:00Z",
                }
            ]
        )

        result = self.table._normalize_collection_data(test_data)

        # Check that partial data is handled correctly
        self.assertEqual(result["parent.id"].iloc[0], 456)
        self.assertIsNone(result["user.id"].iloc[0])  # None user
        self.assertEqual(result["access.level"].iloc[0], 4)
        self.assertIsNone(result["access.draggable"].iloc[0])  # Missing field

    def test_prepare_collection_data(self):
        """Test _prepare_collection_data method"""
        input_data = {
            "title": "Test Collection",
            "description": "Test Description",
            "color": "#FF0000",
            "public": True,
            "parent_id": 123,
        }

        result = self.table._prepare_collection_data(input_data)

        expected = {
            "title": "Test Collection",
            "description": "Test Description",
            "color": "#FF0000",
            "public": True,
            "parent": {"$id": 123},
        }

        self.assertEqual(result, expected)

    def test_get_collections(self):
        """Test get_collections method"""
        self.handler.connection.get_collections.return_value = {
            "result": True,
            "items": [{"_id": 123, "title": "Root Collection"}],
        }
        self.handler.connection.get_child_collections.return_value = {
            "result": True,
            "items": [{"_id": 456, "title": "Child Collection"}],
        }

        result = self.table.get_collections()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["_id"], 123)
        self.assertEqual(result[1]["_id"], 456)

    def test_select_with_simple_filters(self):
        """Test select method with simple WHERE clause conditions for collections"""
        # Mock response with sample collection data
        sample_data = [
            {"_id": 123, "title": "Work Collection", "public": True},
            {"_id": 456, "title": "Personal Collection", "public": False},
        ]

        # Mock the API responses
        self.handler.connection.get_collections.return_value = {"result": True, "items": sample_data}
        self.handler.connection.get_child_collections.return_value = {"result": True, "items": []}

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["_id", "title"],  # selected_columns
                [["=", "public", True]],  # where_conditions - corrected format
                [],  # order_by_conditions
                10,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            filtered_df = pd.DataFrame([{"_id": 123, "title": "Work Collection", "public": True}])
            mock_executor_instance.execute_query.return_value = filtered_df
            mock_executor.return_value = mock_executor_instance

            query = Mock()
            result = self.table.select(query)

            # Should filter to only public collections
            self.assertEqual(len(result), 1)
            self.assertEqual(result["_id"].iloc[0], 123)

    def test_select_with_title_filter(self):
        """Test select method with title filtering for collections"""
        # Mock response with sample collection data
        sample_data = [
            {"_id": 123, "title": "Work Collection"},
            {"_id": 456, "title": "Personal Collection"},
        ]

        # Mock the API responses
        self.handler.connection.get_collections.return_value = {"result": True, "items": sample_data}
        self.handler.connection.get_child_collections.return_value = {"result": True, "items": []}

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["_id", "title"],  # selected_columns
                [["like", "title", "%Work%"]],  # where_conditions - corrected format
                [],  # order_by_conditions
                None,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            filtered_df = pd.DataFrame([{"_id": 123, "title": "Work Collection"}])
            mock_executor_instance.execute_query.return_value = filtered_df
            mock_executor.return_value = mock_executor_instance

            query = Mock()
            result = self.table.select(query)

            # Should filter to collections with "Work" in title
        self.assertEqual(len(result), 1)
        self.assertEqual(result["title"].iloc[0], "Work Collection")


class TestTagsTable(unittest.TestCase):
    """Test cases for TagsTable"""

    def setUp(self):
        self.handler = Mock()
        self.handler.connection = Mock()
        self.table = TagsTable(self.handler)

    def test_get_columns(self):
        """Test get_columns method"""
        columns = self.table.get_columns()

        expected_columns = [
            "_id",
            "label",
            "count",
            "created",
            "lastUpdate",
        ]

        self.assertEqual(columns, expected_columns)

    def test_select_basic(self):
        """Test basic select operation"""
        # Mock API response
        sample_data = [
            {
                "_id": "tag1",
                "label": "Python",
                "count": 15,
                "created": "2024-01-01T00:00:00Z",
                "lastUpdate": "2024-01-01T00:00:00Z",
            },
            {
                "_id": "tag2",
                "label": "JavaScript",
                "count": 8,
                "created": "2024-02-01T00:00:00Z",
                "lastUpdate": "2024-02-01T00:00:00Z",
            },
            {
                "_id": "tag3",
                "label": "Machine Learning",
                "count": 3,
                "created": "2024-03-01T00:00:00Z",
                "lastUpdate": "2024-03-01T00:00:00Z",
            },
        ]

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = ([], [], [], None)
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            # Create a DataFrame with the sample data for the executor to return
            sample_df = pd.DataFrame(sample_data)
            mock_executor_instance.execute_query.return_value = sample_df
            mock_executor.return_value = mock_executor_instance

            # Mock the handler connection's get_tags method
            self.handler.connection.get_tags.return_value = {"items": sample_data}

            query = Mock()
            result = self.table.select(query)

            # Should return DataFrame with all expected columns
            expected_columns = self.table.get_columns()
            for col in expected_columns:
                self.assertIn(col, result.columns, f"Missing column: {col}")

            # Should have the sample data
            self.assertEqual(len(result), 3)
            self.assertListEqual(result["label"].tolist(), ["Python", "JavaScript", "Machine Learning"])

    def test_select_with_filters(self):
        """Test select with filtering"""
        sample_data = [
            {
                "_id": "tag1",
                "label": "Python",
                "count": 15,
                "created": "2024-01-01T00:00:00Z",
                "lastUpdate": "2024-01-01T00:00:00Z",
            },
            {
                "_id": "tag2",
                "label": "JavaScript",
                "count": 8,
                "created": "2024-02-01T00:00:00Z",
                "lastUpdate": "2024-02-01T00:00:00Z",
            },
        ]

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["label", "count"],  # selected_columns
                [["=", "count", 15]],  # where_conditions
                [],  # order_by_conditions
                None,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            filtered_df = pd.DataFrame([{"label": "Python", "count": 15}])
            mock_executor_instance.execute_query.return_value = filtered_df
            mock_executor.return_value = mock_executor_instance

            # Mock the handler connection's get_tags method
            self.handler.connection.get_tags.return_value = {"items": sample_data}

            query = Mock()
            result = self.table.select(query)

            # Should filter to tags with count = 15
            self.assertEqual(len(result), 1)
            self.assertEqual(result["label"].iloc[0], "Python")

    def test_select_with_limit(self):
        """Test select with LIMIT clause"""
        sample_data = [
            {
                "_id": "tag1",
                "label": "Python",
                "count": 15,
                "created": "2024-01-01T00:00:00Z",
                "lastUpdate": "2024-01-01T00:00:00Z",
            },
            {
                "_id": "tag2",
                "label": "JavaScript",
                "count": 8,
                "created": "2024-02-01T00:00:00Z",
                "lastUpdate": "2024-02-01T00:00:00Z",
            },
            {
                "_id": "tag3",
                "label": "Machine Learning",
                "count": 3,
                "created": "2024-03-01T00:00:00Z",
                "lastUpdate": "2024-03-01T00:00:00Z",
            },
        ]

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["label", "count"],  # selected_columns
                [],  # where_conditions
                [],  # order_by_conditions
                2,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            limited_df = pd.DataFrame([{"label": "Python", "count": 15}, {"label": "JavaScript", "count": 8}])
            mock_executor_instance.execute_query.return_value = limited_df
            mock_executor.return_value = mock_executor_instance

            # Mock the handler connection's get_tags method
            self.handler.connection.get_tags.return_value = {"items": sample_data}

            query = Mock()
            result = self.table.select(query)

            # Should limit to 2 results
            self.assertEqual(len(result), 2)
            self.assertListEqual(result["label"].tolist(), ["Python", "JavaScript"])

    def test_insert_not_supported(self):
        """Test that insert operation raises NotImplementedError"""
        with self.assertRaises(NotImplementedError) as context:
            query = Mock()
            self.table.insert(query)

        self.assertIn("Direct tag creation is not supported", str(context.exception))
        self.assertIn("Raindrop.io API", str(context.exception))

    def test_update_not_supported(self):
        """Test that update operation raises NotImplementedError"""
        with self.assertRaises(NotImplementedError) as context:
            query = Mock()
            self.table.update(query)

        self.assertIn("Direct tag updates are not supported", str(context.exception))
        self.assertIn("Raindrop.io API", str(context.exception))

    def test_delete_not_supported(self):
        """Test that delete operation raises NotImplementedError"""
        with self.assertRaises(NotImplementedError) as context:
            query = Mock()
            self.table.delete(query)

        self.assertIn("Tag deletion is not supported", str(context.exception))
        self.assertIn("Raindrop.io API", str(context.exception))

    def test_normalize_tags_data(self):
        """Test _normalize_tags_data method"""
        test_data = pd.DataFrame(
            [
                {
                    "_id": "tag1",
                    "label": "Python",
                    "count": 15,
                    "created": "2024-01-01T00:00:00Z",
                    "lastUpdate": "2024-01-02T00:00:00Z",
                }
            ]
        )

        result = self.table._normalize_tags_data(test_data)

        # Check that dates are converted to datetime
        self.assertEqual(result["label"].iloc[0], "Python")
        self.assertEqual(result["count"].iloc[0], 15)
        # Note: Date conversion would require pandas datetime conversion, checking basic structure
        self.assertIn("_id", result.columns)
        self.assertIn("label", result.columns)
        self.assertIn("count", result.columns)
        self.assertIn("created", result.columns)
        self.assertIn("lastUpdate", result.columns)

    def test_normalize_tags_data_empty(self):
        """Test _normalize_tags_data with empty DataFrame"""
        empty_df = pd.DataFrame()

        result = self.table._normalize_tags_data(empty_df)

        # Should return the same empty DataFrame
        self.assertTrue(result.empty)

    def test_get_tags_calls_api(self):
        """Test that get_tags calls the API correctly"""
        expected_response = {
            "items": [
                {
                    "_id": "tag1",
                    "label": "Python",
                    "count": 15,
                    "created": "2024-01-01T00:00:00Z",
                    "lastUpdate": "2024-01-01T00:00:00Z",
                },
                {
                    "_id": "tag2",
                    "label": "JavaScript",
                    "count": 8,
                    "created": "2024-02-01T00:00:00Z",
                    "lastUpdate": "2024-02-01T00:00:00Z",
                },
            ]
        }

        self.handler.connection.get_tags.return_value = expected_response

        result = self.table.get_tags()

        # Should have called get_tags on the connection
        self.handler.connection.get_tags.assert_called_once()
        # Should return the items from the response
        self.assertEqual(result, expected_response["items"])


class TestParseTable(unittest.TestCase):
    """Test cases for ParseTable"""

    def setUp(self):
        self.handler = Mock()
        self.handler.connection = Mock()
        self.table = ParseTable(self.handler)

    def test_get_columns(self):
        """Test get_columns method"""
        columns = self.table.get_columns()

        expected_columns = [
            "parsed_url",
            "title",
            "excerpt",
            "domain",
            "type",
            "cover",
            "media",
            "lastUpdate",
            "error",
        ]

        self.assertEqual(columns, expected_columns)

    def test_select_single_url(self):
        """Test select with single URL to parse"""
        # Mock API response
        mock_parsed_data = {
            "title": "Test Article",
            "excerpt": "This is a test article excerpt",
            "domain": "example.com",
            "type": "article",
            "cover": "https://example.com/cover.jpg",
            "media": [{"link": "https://example.com/image.jpg"}],
            "lastUpdate": "2024-01-01T00:00:00Z",
        }

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["parsed_url", "title", "excerpt"],  # selected_columns
                [["=", "url", "https://example.com/test"]],  # where_conditions
                [],  # order_by_conditions
                None,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            # Create DataFrame with expected parsed data
            expected_df = pd.DataFrame(
                [
                    {
                        "parsed_url": "https://example.com/test",
                        "title": "Test Article",
                        "excerpt": "This is a test article excerpt",
                        "domain": "example.com",
                        "type": "article",
                        "cover": "https://example.com/cover.jpg",
                        "media": [{"link": "https://example.com/image.jpg"}],
                        "lastUpdate": "2024-01-01T00:00:00Z",
                        "error": None,
                    }
                ]
            )
            mock_executor_instance.execute_query.return_value = expected_df
            mock_executor.return_value = mock_executor_instance

            # Mock the API call
            self.handler.connection.parse_url.return_value = {"result": True, "item": mock_parsed_data}

            query = Mock()
            result = self.table.select(query)

            # Verify API was called with correct URL
            self.handler.connection.parse_url.assert_called_once_with("https://example.com/test")

            # Should return DataFrame with parsed data
            self.assertEqual(len(result), 1)
            self.assertEqual(result["parsed_url"].iloc[0], "https://example.com/test")
            self.assertEqual(result["title"].iloc[0], "Test Article")

    def test_select_multiple_urls(self):
        """Test select with multiple URLs using IN operator"""
        urls = ["https://example1.com", "https://example2.com"]

        # Mock API responses for each URL
        mock_responses = [
            {
                "result": True,
                "item": {"title": "Article 1", "excerpt": "Excerpt 1", "domain": "example1.com", "type": "article"},
            },
            {
                "result": True,
                "item": {"title": "Article 2", "excerpt": "Excerpt 2", "domain": "example2.com", "type": "article"},
            },
        ]

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["parsed_url", "title"],  # selected_columns
                [["in", "url", urls]],  # where_conditions
                [],  # order_by_conditions
                None,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            expected_df = pd.DataFrame(
                [
                    {"parsed_url": "https://example1.com", "title": "Article 1", "error": None},
                    {"parsed_url": "https://example2.com", "title": "Article 2", "error": None},
                ]
            )
            mock_executor_instance.execute_query.return_value = expected_df
            mock_executor.return_value = mock_executor_instance

            # Mock the API calls
            self.handler.connection.parse_url.side_effect = mock_responses

            query = Mock()
            result = self.table.select(query)

            # Verify API was called for each URL
            self.assertEqual(self.handler.connection.parse_url.call_count, 2)
            calls = self.handler.connection.parse_url.call_args_list
            self.assertEqual(calls[0][0][0], "https://example1.com")
            self.assertEqual(calls[1][0][0], "https://example2.com")

            # Should return DataFrame with both parsed URLs
            self.assertEqual(len(result), 2)

    def test_select_no_url_specified(self):
        """Test select without URL specification raises error"""
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["parsed_url", "title"],  # selected_columns
                [],  # where_conditions - no URL specified
                [],  # order_by_conditions
                None,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            query = Mock()
            with self.assertRaises(ValueError) as context:
                self.table.select(query)

            self.assertIn("Please specify URL(s) to parse", str(context.exception))
            self.assertIn("WHERE url =", str(context.exception))

    def test_select_api_error_handling(self):
        """Test select handles API errors gracefully"""
        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["parsed_url", "title", "error"],  # selected_columns
                [["=", "url", "https://invalid-url.com"]],  # where_conditions
                [],  # order_by_conditions
                None,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            expected_df = pd.DataFrame([{"parsed_url": "https://invalid-url.com", "title": None, "error": "API Error"}])
            mock_executor_instance.execute_query.return_value = expected_df
            mock_executor.return_value = mock_executor_instance

            # Mock API to raise exception
            self.handler.connection.parse_url.side_effect = Exception("API Error")

            query = Mock()
            result = self.table.select(query)

            # Should handle error gracefully and return error info
            self.assertEqual(len(result), 1)
            self.assertEqual(result["parsed_url"].iloc[0], "https://invalid-url.com")
            self.assertEqual(result["error"].iloc[0], "API Error")

    def test_select_with_limit(self):
        """Test select with LIMIT clause"""
        urls = ["https://example1.com", "https://example2.com", "https://example3.com"]

        # Mock the SELECT query components
        with (
            patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser") as mock_parser,
            patch(
                "mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor"
            ) as mock_executor,
        ):
            mock_parser_instance = Mock()
            mock_parser_instance.parse_query.return_value = (
                ["parsed_url", "title"],  # selected_columns
                [["in", "url", urls]],  # where_conditions
                [],  # order_by_conditions
                2,  # result_limit
            )
            mock_parser.return_value = mock_parser_instance

            mock_executor_instance = Mock()
            expected_df = pd.DataFrame(
                [
                    {"parsed_url": "https://example1.com", "title": "Article 1"},
                    {"parsed_url": "https://example2.com", "title": "Article 2"},
                ]
            )
            mock_executor_instance.execute_query.return_value = expected_df
            mock_executor.return_value = mock_executor_instance

            # Mock API calls
            mock_responses = [
                {"result": True, "item": {"title": "Article 1", "excerpt": "Excerpt 1"}},
                {"result": True, "item": {"title": "Article 2", "excerpt": "Excerpt 2"}},
                {"result": True, "item": {"title": "Article 3", "excerpt": "Excerpt 3"}},
            ]
            self.handler.connection.parse_url.side_effect = mock_responses

            query = Mock()
            result = self.table.select(query)

            # Should limit to 2 results
            self.assertEqual(len(result), 2)

    def test_insert_not_supported(self):
        """Test that insert operation raises NotImplementedError"""
        with self.assertRaises(NotImplementedError) as context:
            query = Mock()
            self.table.insert(query)

        self.assertIn("URL parsing is a read-only operation", str(context.exception))

    def test_update_not_supported(self):
        """Test that update operation raises NotImplementedError"""
        with self.assertRaises(NotImplementedError) as context:
            query = Mock()
            self.table.update(query)

        self.assertIn("URL parsing is a read-only operation", str(context.exception))

    def test_delete_not_supported(self):
        """Test that delete operation raises NotImplementedError"""
        with self.assertRaises(NotImplementedError) as context:
            query = Mock()
            self.table.delete(query)

        self.assertIn("URL parsing is a read-only operation", str(context.exception))

    def test_normalize_parse_data(self):
        """Test _normalize_parse_data method"""
        test_data = pd.DataFrame(
            [
                {
                    "parsed_url": "https://example.com",
                    "title": "Test Article",
                    "excerpt": "Test excerpt",
                    "domain": "example.com",
                    "lastUpdate": "2024-01-01T00:00:00Z",
                }
            ]
        )

        result = self.table._normalize_parse_data(test_data)

        # Check that all expected columns exist
        expected_columns = self.table.get_columns()
        for col in expected_columns:
            self.assertIn(col, result.columns, f"Missing column: {col}")

        # Check specific values
        self.assertEqual(result["parsed_url"].iloc[0], "https://example.com")
        self.assertEqual(result["title"].iloc[0], "Test Article")

    def test_normalize_parse_data_empty(self):
        """Test _normalize_parse_data with empty DataFrame"""
        empty_df = pd.DataFrame()

        result = self.table._normalize_parse_data(empty_df)

        # Should return the same empty DataFrame
        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
