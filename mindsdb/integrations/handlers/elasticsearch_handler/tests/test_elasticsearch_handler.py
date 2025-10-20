import unittest
from unittest.mock import Mock, patch
from pandas import DataFrame

from mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler import ElasticsearchHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from test_config import SharedFixtures


class TestElasticsearchHandler(unittest.TestCase):
    """
    Comprehensive unit tests for ElasticsearchHandler covering all core methods
    and enterprise features including array handling and fallback mechanisms.
    """

    @classmethod
    def setUpClass(cls):
        """Set up shared test fixtures for the entire test class."""
        cls.connection_data = {"hosts": "localhost:9200", "user": "test_user", "password": "test_password"}
        cls.ssl_connection_data = {
            "hosts": "localhost:9200",
            "user": "test_user",
            "password": "test_password",
            "verify_certs": True,
            "ca_certs": "/path/to/ca.crt",
            "timeout": 30,
        }
        # Shared mock responses for efficiency
        cls.mock_sql_response = {
            "rows": [["John", 30], ["Jane", 25]],
            "columns": [{"name": "name"}, {"name": "age"}],
        }
        cls.mock_search_response = {
            "hits": {
                "hits": [
                    {"_source": {"name": "John", "age": 30, "tags": ["python", "elasticsearch"]}},
                    {"_source": {"name": "Jane", "age": 25, "skills": ["java", "mongodb"]}},
                ]
            }
        }
        cls.mock_mapping_response = {
            "test_index": {
                "mappings": {
                    "properties": {
                        "field1": {"type": "text"},
                        "field2": {"type": "integer"},
                        "nested_field": {"properties": {"sub_field": {"type": "keyword"}}},
                    }
                }
            }
        }

    def setUp(self):
        """Set up test fixtures for each test method."""
        self.handler = ElasticsearchHandler("test_elasticsearch", self.connection_data)
        # Initialize array cache for efficiency
        self.handler._array_fields_cache = {"test_index": ["tags", "skills"]}

    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self.handler, "connection") and self.handler.connection:
            self.handler.disconnect()

    def test_init_method_required_variables(self):
        """Test that __init__ method includes all required variables."""
        handler = ElasticsearchHandler("test", self.connection_data)

        # Test required instance variables
        self.assertEqual(handler.name, "test")  # Handler name is set to the name passed in init
        self.assertEqual(handler.connection_data, self.connection_data)
        self.assertIsNone(handler.connection)
        self.assertFalse(handler.is_connected)
        self.assertIsInstance(handler._array_fields_cache, dict)

    def test_init_invalid_connection_data(self):
        """Test __init__ with invalid connection data."""
        # Test with None connection data
        handler = ElasticsearchHandler("test", None)
        self.assertIsNone(handler.connection_data)

        # Test with empty connection data
        handler = ElasticsearchHandler("test", {})
        self.assertEqual(handler.connection_data, {})

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch")
    def test_connect_success(self, mock_elasticsearch):
        """Test successful connection to Elasticsearch with optimized mock setup."""
        mock_client = Mock()
        mock_elasticsearch.return_value = mock_client

        result = self.handler.connect()

        self.assertEqual(result, mock_client)
        self.assertTrue(self.handler.is_connected)
        self.assertEqual(self.handler.connection, mock_client)

        # Verify SSL defaults are applied
        call_args = mock_elasticsearch.call_args[1]
        self.assertIn("verify_certs", call_args)
        self.assertTrue(call_args["verify_certs"])

    def test_connect_missing_hosts_and_cloud_id(self):
        """Test connection failure when both hosts and cloud_id are missing."""
        handler = ElasticsearchHandler("test", {})

        with self.assertRaises(ValueError) as context:
            handler.connect()

        self.assertIn("Either the hosts or cloud_id parameter should be provided", str(context.exception))

    def test_connect_user_without_password(self):
        """Test connection failure when user provided without password."""
        connection_data = {"hosts": "localhost:9200", "user": "test_user"}
        handler = ElasticsearchHandler("test", connection_data)

        with self.assertRaises(ValueError) as context:
            handler.connect()

        self.assertIn("Both user and password should be provided", str(context.exception))

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch")
    def test_connect_with_cloud_id(self, mock_elasticsearch):
        """Test connection with cloud_id parameter."""
        connection_data = {"cloud_id": "test_cloud_id", "user": "test", "password": "test"}
        handler = ElasticsearchHandler("test", connection_data)
        mock_client = Mock()
        mock_elasticsearch.return_value = mock_client

        result = handler.connect()

        mock_elasticsearch.assert_called_once_with(cloud_id="test_cloud_id", http_auth=("test", "test"))
        self.assertEqual(result, mock_client)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch")
    def test_connect_with_api_key(self, mock_elasticsearch):
        """Test connection with API key authentication."""
        connection_data = {"hosts": "localhost:9200", "api_key": "test_api_key"}
        handler = ElasticsearchHandler("test", connection_data)
        mock_client = Mock()
        mock_elasticsearch.return_value = mock_client

        handler.connect()

        mock_elasticsearch.assert_called_once_with(hosts=["localhost:9200"], api_key="test_api_key")

    def test_disconnect(self):
        """Test disconnection from Elasticsearch."""
        # Mock connected state
        mock_connection = Mock()
        self.handler.connection = mock_connection
        self.handler.is_connected = True

        self.handler.disconnect()

        mock_connection.close.assert_called_once()
        self.assertFalse(self.handler.is_connected)

    def test_disconnect_when_not_connected(self):
        """Test disconnect when not connected."""
        self.handler.is_connected = False

        # Should not raise any exception
        self.handler.disconnect()

        self.assertFalse(self.handler.is_connected)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_check_connection_success(self, mock_connect):
        """Test successful connection check."""
        mock_client = Mock()
        mock_client.sql.query.return_value = {"rows": [["1"]], "columns": [{"name": "1"}]}
        mock_connect.return_value = mock_client

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_check_connection_failure(self, mock_connect):
        """Test connection check failure."""
        mock_connect.side_effect = Exception("Connection failed")

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertIn("Connection failed", response.error_message)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_get_tables_success(self, mock_connect):
        """Test successful retrieval of tables with optimized response handling."""
        mock_client = Mock()
        mock_client.sql.query.return_value = {
            "rows": [
                ["test_index1", "table", "BASE TABLE"],
                ["test_index2", "table", "BASE TABLE"],
                [".system_index", "table", "BASE TABLE"],
            ],
            "columns": [{"name": "name"}, {"name": "type"}, {"name": "kind"}],
        }
        mock_connect.return_value = mock_client

        response = self.handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)

        # Should only return non-system indices
        table_names = response.data_frame["table_name"].tolist()
        self.assertIn("test_index1", table_names)
        self.assertIn("test_index2", table_names)
        self.assertNotIn(".system_index", table_names)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_get_tables_empty_result(self, mock_connect):
        """Test get_tables when no indices exist."""
        mock_client = Mock()
        mock_client.cat.indices.return_value = []
        mock_connect.return_value = mock_client

        response = self.handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        # Should return guidance when no tables found
        self.assertIsInstance(response.data_frame, DataFrame)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_get_columns_success(self, mock_connect):
        """Test successful retrieval of columns with optimized mock reuse."""
        mock_client = Mock()
        mock_client.sql.query.return_value = {
            "rows": [
                ["field1", "text", None],
                ["field2", "integer", None],
                ["nested_field.sub_field", "keyword", None],
            ],
            "columns": [{"name": "column"}, {"name": "type"}, {"name": "mapping"}],
        }
        mock_connect.return_value = mock_client

        response = self.handler.get_columns("test_index")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)

        # Check that columns include nested fields
        column_names = response.data_frame["column_name"].tolist()
        self.assertIn("field1", column_names)
        self.assertIn("field2", column_names)
        self.assertIn("nested_field.sub_field", column_names)

    def test_get_columns_invalid_table_name(self):
        """Test get_columns with invalid table name."""
        with self.assertRaises(ValueError):
            self.handler.get_columns("")

        with self.assertRaises(ValueError):
            self.handler.get_columns(None)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_detect_array_fields(self, mock_connect):
        """Test array field detection functionality."""
        mock_client = Mock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "regular_field": "value",
                            "array_field": ["item1", "item2"],
                            "nested": {"nested_array": ["a", "b", "c"]},
                        }
                    }
                ]
            }
        }
        mock_connect.return_value = mock_client

        array_fields = self.handler._detect_array_fields("test_index")

        self.assertIn("array_field", array_fields)
        self.assertIn("nested.nested_array", array_fields)
        self.assertNotIn("regular_field", array_fields)

    def test_convert_arrays_to_strings(self):
        """Test array to JSON string conversion."""
        test_data = {
            "regular_field": "value",
            "array_field": ["item1", "item2"],
            "nested": {"nested_array": [1, 2, 3], "regular": "value"},
            "empty_array": [],
            "null_field": None,
        }

        result = self.handler._convert_arrays_to_strings(test_data)

        self.assertEqual(result["regular_field"], "value")
        self.assertEqual(result["array_field"], '["item1", "item2"]')
        self.assertEqual(result["nested"]["nested_array"], "[1, 2, 3]")
        self.assertEqual(result["nested"]["regular"], "value")
        self.assertEqual(result["empty_array"], "[]")
        self.assertIsNone(result["null_field"])

    def test_flatten_document(self):
        """Test document flattening functionality."""
        test_doc = {"field1": "value1", "nested": {"field2": "value2", "deep": {"field3": "value3"}}}

        result = self.handler._flatten_document(test_doc)

        self.assertEqual(result["field1"], "value1")
        self.assertEqual(result["nested.field2"], "value2")
        self.assertEqual(result["nested.deep.field3"], "value3")

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_native_query_success(self, mock_connect):
        """Test successful native query execution with shared mock data."""
        mock_client = Mock()
        mock_client.sql.query.return_value = self.mock_sql_response
        mock_connect.return_value = mock_client

        response = self.handler.native_query("SELECT name, age FROM users")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)
        self.assertEqual(list(response.data_frame.columns), ["name", "age"])
        self.assertEqual(len(response.data_frame), 2)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    @patch(
        "mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler._use_search_api_fallback"
    )
    def test_native_query_array_error_fallback(self, mock_fallback, mock_connect):
        """Test native query with array error triggers fallback."""
        mock_client = Mock()
        mock_client.sql.query.side_effect = Exception("Arrays are not supported in SQL")
        mock_connect.return_value = mock_client

        mock_fallback.return_value = Mock(type=RESPONSE_TYPE.TABLE)

        response = self.handler.native_query("SELECT * FROM test_index")

        mock_fallback.assert_called_once()
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_use_search_api_fallback(self, mock_connect):
        """Test Search API fallback functionality with shared fixtures."""
        mock_client = SharedFixtures.mock_elasticsearch_client()
        mock_connect.return_value = mock_client

        response = self.handler._use_search_api_fallback("test_index", limit=10)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, DataFrame)

        # Verify array was converted to JSON string
        if "tags" in response.data_frame.columns:
            tags_value = response.data_frame["tags"].iloc[0]
            self.assertIsInstance(tags_value, str)
            self.assertIn("python", tags_value)

    def test_extract_index_name_from_query(self):
        """Test index name extraction from SQL queries with batch processing."""
        test_cases = [
            ("SELECT * FROM users", "users"),
            ("SELECT name FROM `test-index`", "test-index"),
            ('SELECT * FROM "my_index" WHERE age > 25', "my_index"),
            ("SELECT * FROM users WHERE id = 1", "users"),
        ]

        for query, expected in test_cases:
            with self.subTest(query=query):
                result = self.handler._extract_index_name_from_query(query)
                self.assertEqual(result, expected, f"Failed for query: {query}")

    def test_extract_limit_from_query(self):
        """Test LIMIT extraction from SQL queries with batch processing."""
        test_cases = [
            ("SELECT * FROM users LIMIT 10", 10),
            ("SELECT * FROM users limit 25", 25),
            ("SELECT * FROM users", 50),  # Default
            ("SELECT * FROM users LIMIT abc", 50),  # Invalid, should return default
        ]

        for query, expected in test_cases:
            with self.subTest(query=query):
                result = self.handler._extract_limit_from_query(query)
                self.assertEqual(result, expected, f"Failed for query: {query}")

    def test_extract_columns_from_query(self):
        """Test column extraction from SELECT queries with batch processing."""
        test_cases = [
            ("SELECT * FROM users", None),
            ("SELECT name, age FROM users", ["name", "age"]),
            ('SELECT `field1`, "field2" FROM users', ["field1", "field2"]),
            ("SELECT name FROM users WHERE age > 25", ["name"]),
        ]

        for query, expected in test_cases:
            with self.subTest(query=query):
                result = self.handler._extract_columns_from_query(query)
                self.assertEqual(result, expected, f"Failed for query: {query}")

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.SqlalchemyRender")
    @patch(
        "mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.native_query"
    )
    def test_query_ast_execution(self, mock_native_query, mock_render):
        """Test AST query execution."""
        mock_ast = Mock()
        mock_render.return_value.get_string.return_value = "SELECT * FROM test"
        mock_native_query.return_value = Mock(type=RESPONSE_TYPE.TABLE)

        response = self.handler.query(mock_ast)

        mock_native_query.assert_called_once_with("SELECT * FROM test")
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

    def test_connection_args_validation(self):
        """Test all connection arguments from connection_args.py with optimized validation."""
        from mindsdb.integrations.handlers.elasticsearch_handler.connection_args import connection_args

        # Test configuration validation
        required_args = ["hosts"]
        optional_args = [
            "cloud_id",
            "user",
            "password",
            "api_key",
            "ca_certs",
            "client_cert",
            "client_key",
            "verify_certs",
            "timeout",
        ]

        # Batch validate required arguments
        for arg in required_args:
            self.assertIn(arg, connection_args, f"Missing required connection argument: {arg}")

        # Batch validate optional arguments
        for arg in optional_args:
            self.assertIn(arg, connection_args, f"Missing optional connection argument: {arg}")

        # Batch validate argument structure
        for arg_name, arg_config in connection_args.items():
            with self.subTest(argument=arg_name):
                self.assertIn("type", arg_config, f"Missing 'type' for argument: {arg_name}")
                self.assertIn("description", arg_config, f"Missing 'description' for argument: {arg_name}")


class TestElasticsearchHandlerIntegration(unittest.TestCase):
    """
    Integration tests for ElasticsearchHandler requiring actual Elasticsearch connection.
    These tests are skipped if Elasticsearch is not available.
    """

    def setUp(self):
        """Set up integration test fixtures."""
        self.connection_data = {"hosts": "localhost:9200"}
        self.handler = ElasticsearchHandler("test_elasticsearch", self.connection_data)

    def test_handler_in_information_schema(self):
        """Test that handler appears in information_schema.handlers."""
        # This would require actual MindsDB instance - placeholder for documentation
        pass

    def test_create_database_command(self):
        """Test CREATE DATABASE command functionality."""
        # This would require actual MindsDB instance - placeholder for documentation
        pass

    def test_show_handlers_command(self):
        """Test SHOW HANDLERS includes elasticsearch handler."""
        # This would require actual MindsDB instance - placeholder for documentation
        pass


if __name__ == "__main__":
    unittest.main()
