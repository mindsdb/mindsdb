import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
from elasticsearch.exceptions import ConnectionError as ESConnectionError

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler import (
    ElasticsearchHandler,
)
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class MockElasticsearchClient(Mock):
    """Mock Elasticsearch client for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = MagicMock()
        self.search = MagicMock()
        self.indices = MagicMock()
        self.ping = MagicMock(return_value=True)


class TestElasticsearchHandler(BaseDatabaseHandlerTest, unittest.TestCase):
    """Unit tests for Elasticsearch handler"""

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            hosts="localhost:9200",
            user="elastic",
            password="changeme",
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return ESConnectionError("N/A", "Connection Failed", {})

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch")

    def create_handler(self):
        return ElasticsearchHandler(name="test_elasticsearch", connection_data=self.dummy_connection_data)

    @property
    def get_tables_query(self):
        """Elasticsearch doesn't use SQL for listing tables, returns placeholder"""
        return "SHOW TABLES"

    @property
    def get_columns_query(self):
        """Elasticsearch doesn't use SQL for listing columns, returns placeholder"""
        return f"DESCRIBE {self.mock_table}"

    def test_connect_with_ssl(self):
        """Test connection with SSL/TLS parameters"""
        ssl_connection_data = OrderedDict(
            hosts="localhost:9200",
            user="elastic",
            password="changeme",
            ca_certs="/path/to/ca.crt",
            client_cert="/path/to/client.crt",
            client_key="/path/to/client.key",
            verify_certs=True,
        )
        handler = ElasticsearchHandler(name="test_ssl", connection_data=ssl_connection_data)

        with patch(
            "mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch"
        ) as mock_es:
            mock_es.return_value = MockElasticsearchClient()
            connection = handler.connect()
            self.assertIsNotNone(connection)
            self.assertTrue(handler.is_connected)

    def test_connect_with_cloud_id(self):
        """Test connection with Elastic Cloud ID"""
        cloud_connection_data = OrderedDict(
            cloud_id="deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJGFiY2RlZg==",
            api_key="api_key_value",
        )
        handler = ElasticsearchHandler(name="test_cloud", connection_data=cloud_connection_data)

        with patch(
            "mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch"
        ) as mock_es:
            mock_es.return_value = MockElasticsearchClient()
            connection = handler.connect()
            self.assertIsNotNone(connection)

    def test_native_query_with_sql_api(self):
        """Test native_query using Elasticsearch SQL API"""
        mock_client = MockElasticsearchClient()
        mock_client.sql.query.return_value = {
            "columns": [{"name": "id"}, {"name": "name"}],
            "rows": [[1, "test1"], [2, "test2"]],
        }
        self.mock_connect.return_value = mock_client

        query = "SELECT id, name FROM products LIMIT 2"
        response = self.handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)
        self.assertListEqual(list(response.data_frame.columns), ["id", "name"])

    def test_native_query_fallback_to_search_api(self):
        """Test automatic fallback to Search API when SQL API fails with array error"""
        mock_client = MockElasticsearchClient()

        # SQL API fails with array error
        mock_client.sql.query.side_effect = Exception("Arrays are not supported")

        # Search API returns results with arrays converted to JSON
        mock_client.search.return_value = {
            "hits": {
                "hits": [{"_source": {"id": 1, "tags": '["tag1", "tag2"]'}}, {"_source": {"id": 2, "tags": '["tag3"]'}}]
            }
        }

        # Mock index mapping for Search API fallback
        mock_client.indices.get_mapping.return_value = {
            "products": {"mappings": {"properties": {"id": {"type": "long"}, "tags": {"type": "keyword"}}}}
        }

        self.mock_connect.return_value = mock_client

        query = "SELECT id, tags FROM products LIMIT 2"
        response = self.handler.native_query(query)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)

    def test_get_tables(self):
        """Test get_tables returns list of indices"""
        mock_client = MockElasticsearchClient()
        # Mock SQL API response for "SHOW TABLES"
        mock_client.sql.query.return_value = {
            "columns": [{"name": "name"}, {"name": "type"}, {"name": "kind"}, {"name": "catalog"}],
            "rows": [
                ["products", "BASE TABLE", "TABLE", "elasticsearch_test"],
                ["orders", "BASE TABLE", "TABLE", "elasticsearch_test"],
                [".kibana", "BASE TABLE", "TABLE", "elasticsearch_test"],  # System index should be filtered
            ],
        }
        self.mock_connect.return_value = mock_client

        response = self.handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        # Should exclude system indices starting with .
        self.assertEqual(len(response.data_frame), 2)
        table_names = response.data_frame["table_name"].tolist()
        self.assertIn("products", table_names)
        self.assertIn("orders", table_names)
        self.assertNotIn(".kibana", table_names)

    def test_get_columns(self):
        """Test get_columns returns field mappings for an index"""
        mock_client = MockElasticsearchClient()
        # Mock SQL API response for "DESCRIBE products"
        mock_client.sql.query.return_value = {
            "columns": [{"name": "column"}, {"name": "type"}, {"name": "mapping"}],
            "rows": [
                ["id", "BIGINT", None],
                ["name", "TEXT", None],
                ["price", "DOUBLE", None],
                ["created_at", "DATETIME", None],
                ["tags", "KEYWORD", None],
            ],
        }
        self.mock_connect.return_value = mock_client

        response = self.handler.get_columns("products")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 5)

        column_names = response.data_frame["COLUMN_NAME"].tolist()
        self.assertIn("id", column_names)
        self.assertIn("name", column_names)
        self.assertIn("price", column_names)
        self.assertIn("created_at", column_names)
        self.assertIn("tags", column_names)

    def test_get_columns_with_nested_fields(self):
        """Test get_columns flattens nested object fields with dot notation"""
        mock_client = MockElasticsearchClient()
        # Mock SQL API response for "DESCRIBE products" with nested fields
        mock_client.sql.query.return_value = {
            "columns": [{"name": "column"}, {"name": "type"}, {"name": "mapping"}],
            "rows": [
                ["id", "BIGINT", None],
                ["metadata.category", "KEYWORD", None],
                ["metadata.rating", "FLOAT", None],
            ],
        }
        self.mock_connect.return_value = mock_client

        response = self.handler.get_columns("products")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        column_names = response.data_frame["COLUMN_NAME"].tolist()
        self.assertIn("id", column_names)
        self.assertIn("metadata.category", column_names)
        self.assertIn("metadata.rating", column_names)

    def test_get_column_statistics_all_columns(self):
        """Test meta_get_column_statistics_for_table returns stats for all columns"""
        mock_client = MockElasticsearchClient()

        # Mock index mapping
        mock_client.indices.get_mapping.return_value = {
            "products": {
                "mappings": {
                    "properties": {"id": {"type": "long"}, "name": {"type": "keyword"}, "price": {"type": "double"}}
                }
            }
        }

        # Mock aggregation response with hits total for NULL_PERCENTAGE calculation
        mock_client.search.return_value = {
            "hits": {"total": {"value": 100}},
            "aggregations": {
                "id_cardinality": {"value": 100},
                "id_missing": {"doc_count": 0},
                "id_stats": {"min": 1, "max": 100, "avg": 50.5},
                "name_cardinality": {"value": 95},
                "name_missing": {"doc_count": 5},
                "price_cardinality": {"value": 80},
                "price_missing": {"doc_count": 10},
                "price_stats": {"min": 9.99, "max": 999.99, "avg": 99.95},
            },
        }

        self.mock_connect.return_value = mock_client

        response = self.handler.meta_get_column_statistics_for_table("products")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 3)

        expected_columns = {
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "NULL_PERCENTAGE",
            "DISTINCT_VALUES_COUNT",
            "MINIMUM_VALUE",
            "MAXIMUM_VALUE",
        }
        self.assertEqual(set(response.data_frame.columns), expected_columns)

        # Check numeric field has stats
        price_row = response.data_frame[response.data_frame["COLUMN_NAME"] == "price"].iloc[0]
        self.assertEqual(price_row["TABLE_NAME"], "products")
        self.assertEqual(price_row["DATA_TYPE"], "double")
        self.assertEqual(price_row["DISTINCT_VALUES_COUNT"], 80)
        self.assertEqual(price_row["NULL_PERCENTAGE"], 10.0)  # 10/100 * 100 = 10%
        self.assertEqual(price_row["MINIMUM_VALUE"], 9.99)
        self.assertEqual(price_row["MAXIMUM_VALUE"], 999.99)

    def test_get_column_statistics_specific_column(self):
        """Test meta_get_column_statistics_for_table for specific columns"""
        mock_client = MockElasticsearchClient()

        mock_client.indices.get_mapping.return_value = {
            "products": {"mappings": {"properties": {"price": {"type": "double"}}}}
        }

        mock_client.search.return_value = {
            "hits": {"total": {"value": 100}},
            "aggregations": {
                "price_cardinality": {"value": 80},
                "price_missing": {"doc_count": 10},
                "price_stats": {"min": 9.99, "max": 999.99, "avg": 99.95},
            },
        }

        self.mock_connect.return_value = mock_client

        response = self.handler.meta_get_column_statistics_for_table("products", ["price"])

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(response.data_frame), 1)
        self.assertEqual(response.data_frame.iloc[0]["COLUMN_NAME"], "price")

    def test_get_column_statistics_invalid_column(self):
        """Test meta_get_column_statistics_for_table raises error for invalid column"""
        mock_client = MockElasticsearchClient()

        mock_client.indices.get_mapping.return_value = {
            "products": {"mappings": {"properties": {"id": {"type": "long"}}}}
        }

        self.mock_connect.return_value = mock_client

        with self.assertRaises(ValueError) as context:
            self.handler.meta_get_column_statistics_for_table("products", ["nonexistent_column"])

        self.assertIn("not found", str(context.exception).lower())

    def test_get_primary_keys(self):
        """Test meta_get_primary_keys returns _id as primary key"""
        response = self.handler.meta_get_primary_keys(["products"])

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 1)

        expected_columns = {"TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME"}
        self.assertEqual(set(response.data_frame.columns), expected_columns)

        row = response.data_frame.iloc[0]
        self.assertEqual(row["TABLE_NAME"], "products")
        self.assertEqual(row["COLUMN_NAME"], "_id")
        self.assertEqual(row["CONSTRAINT_NAME"], "PRIMARY")

    def test_get_primary_keys_multiple_tables(self):
        """Test meta_get_primary_keys with multiple tables"""
        response = self.handler.meta_get_primary_keys(["products", "orders"])

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(response.data_frame), 2)

        table_names = response.data_frame["TABLE_NAME"].tolist()
        self.assertIn("products", table_names)
        self.assertIn("orders", table_names)

    def test_get_foreign_keys(self):
        """Test meta_get_foreign_keys returns empty DataFrame with correct schema"""
        response = self.handler.meta_get_foreign_keys(["products"])

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 0)

        expected_columns = {
            "CHILD_TABLE_NAME",
            "CHILD_COLUMN_NAME",
            "PARENT_TABLE_NAME",
            "PARENT_COLUMN_NAME",
            "CONSTRAINT_NAME",
        }
        self.assertEqual(set(response.data_frame.columns), expected_columns)

    def test_check_connection_success(self):
        """Test check_connection returns success when Elasticsearch is reachable"""
        mock_client = MockElasticsearchClient()
        mock_client.ping.return_value = True
        self.mock_connect.return_value = mock_client

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)

    def test_check_connection_failure(self):
        """Test check_connection returns error when Elasticsearch is unreachable"""
        self.mock_connect.side_effect = Exception("Connection refused")

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertIsNotNone(response.error_message)
        self.assertIn("Connection refused", response.error_message)


if __name__ == "__main__":
    unittest.main()
