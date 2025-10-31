"""
pytest test suite for Elasticsearch Data Catalog functionality.

This module tests the three Data Catalog methods required for MindsDB enterprise verification:
- get_column_statistics(table_name, column_name=None)
- get_primary_keys(table_name)
- get_foreign_keys(table_name)

Compatible with mindsdb-handlers-monitor framework.

Usage:
    pytest test_data_catalog.py -v
    pytest test_data_catalog.py -m datacatalog
    pytest test_data_catalog.py::TestElasticsearchDataCatalog::test_get_column_statistics_all_columns
"""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler import ElasticsearchHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


@pytest.fixture
def handler():
    """Create handler instance with mock connection data"""
    connection_data = {"hosts": "localhost:9200", "user": "elastic", "password": "changeme"}
    return ElasticsearchHandler(name="test_elasticsearch", connection_data=connection_data)


@pytest.fixture
def mock_elasticsearch():
    """Create mock Elasticsearch connection"""
    mock_es = MagicMock()

    # Mock successful connection
    mock_es.sql.query.return_value = {"rows": [[1]], "columns": [{"name": "result"}]}

    # Mock index mapping
    mock_es.indices.get_mapping.return_value = {
        "test_index": {
            "mappings": {
                "properties": {
                    "id": {"type": "long"},
                    "name": {"type": "keyword"},
                    "price": {"type": "double"},
                    "created_at": {"type": "date"},
                    "description": {"type": "text"},
                    "tags": {"type": "keyword"},  # Array field
                    "metadata": {  # Nested object
                        "properties": {"category": {"type": "keyword"}, "rating": {"type": "float"}}
                    },
                }
            }
        }
    }

    # Mock aggregation response
    mock_es.search.return_value = {
        "aggregations": {
            "id_cardinality": {"value": 100},
            "id_missing": {"doc_count": 0},
            "id_stats": {"min": 1, "max": 100, "avg": 50.5, "sum": 5050, "count": 100},
            "name_cardinality": {"value": 95},
            "name_missing": {"doc_count": 5},
            "price_cardinality": {"value": 75},
            "price_missing": {"doc_count": 10},
            "price_stats": {"min": 9.99, "max": 999.99, "avg": 99.95, "sum": 9995, "count": 100},
            "created_at_cardinality": {"value": 100},
            "created_at_missing": {"doc_count": 0},
            "created_at_stats": {"min": 1609459200000, "max": 1640995200000, "avg": 1625227200000},
            "description_cardinality": {"value": 90},
            "description_missing": {"doc_count": 10},
            "tags_cardinality": {"value": 20},
            "tags_missing": {"doc_count": 5},
            "metadata_category_cardinality": {"value": 10},
            "metadata_category_missing": {"doc_count": 5},
            "metadata_rating_cardinality": {"value": 50},
            "metadata_rating_missing": {"doc_count": 15},
            "metadata_rating_stats": {"min": 1.0, "max": 5.0, "avg": 3.5},
        }
    }

    return mock_es


class TestElasticsearchDataCatalog:
    """Test suite for Data Catalog methods"""

    @pytest.mark.datacatalog
    def test_get_column_statistics_all_columns(self, handler, mock_elasticsearch):
        """Test get_column_statistics returns statistics for all columns"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index")

            assert response.type == RESPONSE_TYPE.TABLE
            assert isinstance(response.data_frame, pd.DataFrame)

            # Check DataFrame structure
            expected_columns = {"column_name", "data_type", "null_count", "distinct_count", "min", "max", "avg"}
            assert set(response.data_frame.columns) == expected_columns

            # Check we got statistics for all fields (including nested)
            assert len(response.data_frame) >= 5  # At least 5 top-level fields

            # Verify numeric field has stats
            numeric_row = response.data_frame[response.data_frame["column_name"] == "price"].iloc[0]
            assert numeric_row["data_type"] == "double"
            assert numeric_row["distinct_count"] == 75
            assert numeric_row["null_count"] == 10
            assert numeric_row["min"] == 9.99
            assert numeric_row["max"] == 999.99
            assert numeric_row["avg"] == 99.95

    @pytest.mark.datacatalog
    def test_get_column_statistics_specific_column(self, handler, mock_elasticsearch):
        """Test get_column_statistics for a specific column"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index", "price")

            assert response.type == RESPONSE_TYPE.TABLE
            assert len(response.data_frame) == 1

            row = response.data_frame.iloc[0]
            assert row["column_name"] == "price"
            assert row["data_type"] == "double"
            assert row["distinct_count"] == 75
            assert row["null_count"] == 10

    @pytest.mark.datacatalog
    def test_get_column_statistics_numeric_aggregations(self, handler, mock_elasticsearch):
        """Test that numeric fields have min/max/avg"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index", "id")

            assert response.type == RESPONSE_TYPE.TABLE
            row = response.data_frame.iloc[0]

            assert row["min"] == 1
            assert row["max"] == 100
            assert row["avg"] == 50.5

    @pytest.mark.datacatalog
    def test_get_column_statistics_keyword_cardinality(self, handler, mock_elasticsearch):
        """Test that keyword fields have cardinality but no min/max/avg"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index", "name")

            assert response.type == RESPONSE_TYPE.TABLE
            row = response.data_frame.iloc[0]

            assert row["data_type"] == "keyword"
            assert row["distinct_count"] == 95
            assert row["null_count"] == 5
            assert pd.isna(row["min"])  # Keyword fields don't have min/max/avg
            assert pd.isna(row["max"])
            assert pd.isna(row["avg"])

    @pytest.mark.datacatalog
    def test_get_column_statistics_null_counts(self, handler, mock_elasticsearch):
        """Test null count calculation"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index")

            assert response.type == RESPONSE_TYPE.TABLE

            # Check null counts are present for all fields
            for _, row in response.data_frame.iterrows():
                assert "null_count" in row
                assert isinstance(row["null_count"], (int, float))
                assert row["null_count"] >= 0

    @pytest.mark.datacatalog
    def test_get_column_statistics_array_fields(self, handler, mock_elasticsearch):
        """Test that array fields are treated as text (cardinality only)"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index", "tags")

            assert response.type == RESPONSE_TYPE.TABLE
            row = response.data_frame.iloc[0]

            # Array field should have cardinality
            assert row["distinct_count"] == 20
            assert row["null_count"] == 5

    @pytest.mark.datacatalog
    def test_get_column_statistics_nested_fields(self, handler, mock_elasticsearch):
        """Test that nested object fields are flattened with dot notation"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index")

            assert response.type == RESPONSE_TYPE.TABLE

            # Check for nested fields with dot notation
            nested_fields = response.data_frame[response.data_frame["column_name"].str.contains("metadata.")]
            assert len(nested_fields) >= 2  # metadata.category and metadata.rating

            # Check nested numeric field has stats
            rating_row = response.data_frame[response.data_frame["column_name"] == "metadata.rating"].iloc[0]
            assert rating_row["min"] == 1.0
            assert rating_row["max"] == 5.0
            assert rating_row["avg"] == 3.5

    @pytest.mark.datacatalog
    def test_get_primary_keys(self, handler):
        """Test get_primary_keys returns _id"""
        response = handler.get_primary_keys("test_index")

        assert response.type == RESPONSE_TYPE.TABLE
        assert isinstance(response.data_frame, pd.DataFrame)

        # Check DataFrame structure
        expected_columns = {"constraint_name", "column_name"}
        assert set(response.data_frame.columns) == expected_columns

        # Check we have exactly one row with _id
        assert len(response.data_frame) == 1
        assert response.data_frame.iloc[0]["column_name"] == "_id"
        assert response.data_frame.iloc[0]["constraint_name"] == "PRIMARY"

    @pytest.mark.datacatalog
    def test_get_foreign_keys(self, handler):
        """Test get_foreign_keys returns empty DataFrame"""
        response = handler.get_foreign_keys("test_index")

        assert response.type == RESPONSE_TYPE.TABLE
        assert isinstance(response.data_frame, pd.DataFrame)

        # Check DataFrame structure
        expected_columns = {"constraint_name", "column_name", "referenced_table", "referenced_column"}
        assert set(response.data_frame.columns) == expected_columns

        # Check DataFrame is empty (NoSQL has no foreign keys)
        assert len(response.data_frame) == 0

    @pytest.mark.datacatalog
    @pytest.mark.error
    def test_get_column_statistics_invalid_table(self, handler, mock_elasticsearch):
        """Test error handling for invalid table name"""
        mock_elasticsearch.indices.get_mapping.side_effect = Exception("Index not found")

        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("nonexistent_index")

            assert response.type == RESPONSE_TYPE.ERROR
            assert "Index not found" in response.error_message or "not found" in response.error_message.lower()

    @pytest.mark.datacatalog
    @pytest.mark.error
    def test_get_column_statistics_invalid_column(self, handler, mock_elasticsearch):
        """Test error handling for invalid column name"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            with pytest.raises(ValueError, match="Column .* not found"):
                handler.get_column_statistics("test_index", "nonexistent_column")

    @pytest.mark.datacatalog
    def test_get_column_statistics_empty_index(self, handler, mock_elasticsearch):
        """Test handling of index with no properties"""
        mock_elasticsearch.indices.get_mapping.return_value = {"empty_index": {"mappings": {"properties": {}}}}

        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("empty_index")

            assert response.type == RESPONSE_TYPE.TABLE
            assert len(response.data_frame) == 0

    @pytest.mark.datacatalog
    def test_get_column_statistics_date_fields(self, handler, mock_elasticsearch):
        """Test that date fields have stats (as timestamps)"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            response = handler.get_column_statistics("test_index", "created_at")

            assert response.type == RESPONSE_TYPE.TABLE
            row = response.data_frame.iloc[0]

            assert row["data_type"] == "date"
            assert row["min"] == 1609459200000  # Timestamp
            assert row["max"] == 1640995200000
            assert row["avg"] == 1625227200000

    @pytest.mark.datacatalog
    @pytest.mark.performance
    def test_get_column_statistics_single_query(self, handler, mock_elasticsearch):
        """Test that statistics are gathered in a single aggregation query"""
        with patch.object(handler, "connect", return_value=mock_elasticsearch):
            handler.get_column_statistics("test_index")

            # Should call search exactly once (single aggregation query)
            assert mock_elasticsearch.search.call_count == 1

            # Verify the search was called with size=0 (only aggregations)
            call_args = mock_elasticsearch.search.call_args
            assert call_args[1]["body"]["size"] == 0

            # Verify aggregations were requested
            assert "aggs" in call_args[1]["body"]

    @pytest.mark.datacatalog
    def test_get_primary_keys_invalid_table(self, handler):
        """Test get_primary_keys with invalid input"""
        with pytest.raises(ValueError, match="Table name must be a non-empty string"):
            handler.get_primary_keys("")

        with pytest.raises(ValueError, match="Table name must be a non-empty string"):
            handler.get_primary_keys(None)

    @pytest.mark.datacatalog
    def test_get_foreign_keys_invalid_table(self, handler):
        """Test get_foreign_keys with invalid input"""
        with pytest.raises(ValueError, match="Table name must be a non-empty string"):
            handler.get_foreign_keys("")

        with pytest.raises(ValueError, match="Table name must be a non-empty string"):
            handler.get_foreign_keys(None)

    @pytest.mark.datacatalog
    def test_extract_fields_from_mapping(self, handler):
        """Test the _extract_fields_from_mapping helper method"""
        properties = {
            "simple_field": {"type": "keyword"},
            "nested_object": {"properties": {"sub_field1": {"type": "text"}, "sub_field2": {"type": "long"}}},
            "deep_nested": {"properties": {"level1": {"properties": {"level2": {"type": "double"}}}}},
        }

        fields = {}
        handler._extract_fields_from_mapping(properties, fields)

        # Check all fields were extracted with correct paths
        assert "simple_field" in fields
        assert "nested_object.sub_field1" in fields
        assert "nested_object.sub_field2" in fields
        assert "deep_nested.level1.level2" in fields

        # Check field types
        assert fields["simple_field"]["type"] == "keyword"
        assert fields["nested_object.sub_field1"]["type"] == "text"
        assert fields["deep_nested.level1.level2"]["type"] == "double"


@pytest.mark.datacatalog
class TestDataCatalogIntegration:
    """
    Integration tests for Data Catalog with real Elasticsearch (optional).

    These tests require a running Elasticsearch instance with Kibana sample data.
    Skip if ELASTICSEARCH_INTEGRATION_TEST environment variable is not set.
    """

    @pytest.fixture
    def real_handler(self):
        """Create handler for real Elasticsearch instance"""
        import os

        if not os.getenv("ELASTICSEARCH_INTEGRATION_TEST"):
            pytest.skip("Integration tests disabled. Set ELASTICSEARCH_INTEGRATION_TEST=1 to enable")

        connection_data = {
            "hosts": os.getenv("ELASTICSEARCH_HOST", "localhost:9200"),
            "user": os.getenv("ELASTICSEARCH_USER", "elastic"),
            "password": os.getenv("ELASTICSEARCH_PASSWORD", "changeme"),
        }
        return ElasticsearchHandler(name="test_elasticsearch_integration", connection_data=connection_data)

    @pytest.mark.integration
    def test_get_column_statistics_flights_dataset(self, real_handler):
        """Test with Kibana sample flights dataset"""
        response = real_handler.get_column_statistics("kibana_sample_data_flights")

        assert response.type == RESPONSE_TYPE.TABLE
        assert len(response.data_frame) > 0

        # Check for known flight dataset fields
        columns = response.data_frame["column_name"].tolist()
        assert any("FlightNum" in col for col in columns)

    @pytest.mark.integration
    def test_get_primary_keys_integration(self, real_handler):
        """Test get_primary_keys with real dataset"""
        response = real_handler.get_primary_keys("kibana_sample_data_flights")

        assert response.type == RESPONSE_TYPE.TABLE
        assert len(response.data_frame) == 1
        assert response.data_frame.iloc[0]["column_name"] == "_id"

    @pytest.mark.integration
    def test_get_foreign_keys_integration(self, real_handler):
        """Test get_foreign_keys with real dataset"""
        response = real_handler.get_foreign_keys("kibana_sample_data_flights")

        assert response.type == RESPONSE_TYPE.TABLE
        assert len(response.data_frame) == 0


# Test configuration for pytest markers
def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line("markers", "datacatalog: Data Catalog functionality tests")
    config.addinivalue_line("markers", "integration: Integration tests requiring real Elasticsearch")
    config.addinivalue_line("markers", "performance: Performance validation tests")
    config.addinivalue_line("markers", "error: Error handling tests")
