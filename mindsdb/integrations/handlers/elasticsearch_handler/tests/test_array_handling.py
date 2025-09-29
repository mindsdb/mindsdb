import unittest
from unittest.mock import Mock, patch
import json

from mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler import ElasticsearchHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class TestArrayHandling(unittest.TestCase):
    """
    Optimized unit tests for array handling functionality in ElasticsearchHandler.
    This is a critical enterprise feature that enables SQL compatibility with
    Elasticsearch data containing arrays.
    """

    @classmethod
    def setUpClass(cls):
        """Set up shared test fixtures for efficiency."""
        cls.connection_data = {"hosts": "localhost:9200"}
        # Shared test data for efficiency
        cls.simple_array_data = {"tags": ["python", "elasticsearch", "data"]}
        cls.nested_array_data = {
            "user": {"skills": ["python", "java"], "projects": ["project1", "project2"]},
            "metadata": {"tags": ["tag1", "tag2"], "categories": ["cat1", "cat2"]},
        }
        cls.mixed_type_data = {
            "mixed_array": ["string", 123, 45.6, True, None],
            "numeric_array": [1, 2, 3, 4, 5],
            "boolean_array": [True, False, True],
        }
        cls.complex_nested_data = {
            "document": {
                "metadata": {
                    "tags": ["important", "archived"],
                    "categories": ["work", "personal"],
                    "authors": [
                        {"name": "John", "roles": ["editor", "reviewer"]},
                        {"name": "Jane", "roles": ["author"]},
                    ],
                }
            }
        }

    def setUp(self):
        """Set up test fixtures for each test."""
        self.handler = ElasticsearchHandler("test_elasticsearch", self.connection_data)

    def test_convert_simple_array_to_string(self):
        """Test conversion of simple arrays to JSON strings."""
        result = self.handler._convert_arrays_to_strings(self.simple_array_data)

        self.assertEqual(result["tags"], '["python", "elasticsearch", "data"]')
        self.assertIsInstance(result["tags"], str)

    def test_convert_nested_arrays_to_strings(self):
        """Test conversion of nested structure with arrays."""
        result = self.handler._convert_arrays_to_strings(self.nested_array_data)

        self.assertEqual(result["user"]["skills"], '["python", "java"]')
        self.assertEqual(result["user"]["projects"], '["project1", "project2"]')
        self.assertEqual(result["metadata"]["tags"], '["tag1", "tag2"]')
        self.assertEqual(result["metadata"]["categories"], '["cat1", "cat2"]')

    def test_convert_mixed_type_arrays(self):
        """Test conversion of arrays with mixed data types."""
        result = self.handler._convert_arrays_to_strings(self.mixed_type_data)

        # Check that arrays are converted to JSON strings
        self.assertIsInstance(result["mixed_array"], str)
        self.assertIsInstance(result["numeric_array"], str)
        self.assertIsInstance(result["boolean_array"], str)

        # Verify JSON can be parsed back
        parsed_mixed = json.loads(result["mixed_array"])
        self.assertEqual(parsed_mixed, ["string", 123, 45.6, True, None])

    def test_convert_empty_arrays(self):
        """Test conversion of empty arrays."""
        test_data = {"empty_array": [], "regular_field": "value"}

        result = self.handler._convert_arrays_to_strings(test_data)

        self.assertEqual(result["empty_array"], "[]")
        self.assertEqual(result["regular_field"], "value")

    def test_preserve_non_array_values(self):
        """Test that non-array values are preserved unchanged."""
        test_data = {
            "string_field": "test string",
            "integer_field": 42,
            "float_field": 3.14,
            "boolean_field": True,
            "null_field": None,
            "nested_object": {"inner_string": "inner value", "inner_number": 100},
        }

        result = self.handler._convert_arrays_to_strings(test_data)

        self.assertEqual(result["string_field"], "test string")
        self.assertEqual(result["integer_field"], 42)
        self.assertEqual(result["float_field"], 3.14)
        self.assertEqual(result["boolean_field"], True)
        self.assertIsNone(result["null_field"])
        self.assertEqual(result["nested_object"]["inner_string"], "inner value")
        self.assertEqual(result["nested_object"]["inner_number"], 100)

    def test_convert_complex_nested_arrays(self):
        """Test conversion of complex nested structures with arrays."""
        result = self.handler._convert_arrays_to_strings(self.complex_nested_data)

        # Check top-level arrays
        self.assertEqual(result["document"]["metadata"]["tags"], '["important", "archived"]')
        self.assertEqual(result["document"]["metadata"]["categories"], '["work", "personal"]')

        # Check that nested arrays within objects are also converted
        authors_json = result["document"]["metadata"]["authors"]
        self.assertIsInstance(authors_json, str)
        parsed_authors = json.loads(authors_json)
        self.assertEqual(len(parsed_authors), 2)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_detect_array_fields_scenarios(self, mock_connect):
        """Test array field detection with both single and multiple document scenarios."""
        mock_client = Mock()

        # Test single document scenario
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "name": "John Doe",
                            "tags": ["developer", "python"],
                            "scores": [85, 90, 78],
                            "metadata": {"skills": ["coding", "testing"]},
                        }
                    }
                ]
            }
        }
        mock_connect.return_value = mock_client

        array_fields = self.handler._detect_array_fields("test_index")

        self.assertIn("tags", array_fields)
        self.assertIn("scores", array_fields)
        self.assertIn("metadata.skills", array_fields)
        self.assertNotIn("name", array_fields)

        # Test multiple document scenario
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "user_id": 1,
                            "tags": ["python", "elasticsearch"],
                            "preferences": {"languages": ["english", "spanish"]},
                        }
                    },
                    {
                        "_source": {
                            "user_id": 2,
                            "tags": ["java", "mongodb"],
                            "categories": ["backend", "database"],
                            "preferences": {"languages": ["french", "german"]},
                        }
                    },
                ]
            }
        }

        # Clear cache for second test
        self.handler._array_fields_cache = {}
        array_fields_multi = self.handler._detect_array_fields("multi_index")

        # Should detect arrays from both documents
        self.assertIn("tags", array_fields_multi)
        self.assertIn("categories", array_fields_multi)
        self.assertIn("preferences.languages", array_fields_multi)
        self.assertNotIn("user_id", array_fields_multi)

    def test_detect_array_fields_caching(self):
        """Test that array field detection results are cached."""
        # Set up cache
        self.handler._array_fields_cache = {"cached_index": ["field1", "field2"]}

        # Should return cached result without calling Elasticsearch
        result = self.handler._detect_array_fields("cached_index")

        self.assertEqual(result, ["field1", "field2"])

    def test_find_arrays_in_document_deep_nesting(self):
        """Test finding arrays in deeply nested documents."""
        test_doc = {
            "level1": {
                "level2": {
                    "level3": {"deep_array": ["item1", "item2"], "regular_field": "value"},
                    "array_level2": ["a", "b"],
                },
                "array_level1": ["x", "y", "z"],
            },
            "top_array": ["top1", "top2"],
        }

        result = self.handler._find_arrays_in_document(test_doc)

        expected_arrays = [
            "level1.level2.level3.deep_array",
            "level1.level2.array_level2",
            "level1.array_level1",
            "top_array",
        ]

        for expected_array in expected_arrays:
            self.assertIn(expected_array, result)

        # Should not include non-array fields
        self.assertNotIn("level1.level2.level3.regular_field", result)

    @patch("mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.ElasticsearchHandler.connect")
    def test_search_api_fallback_with_arrays(self, mock_connect):
        """Test Search API fallback properly handles arrays."""
        mock_client = Mock()
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "id": 1,
                            "name": "John",
                            "skills": ["python", "elasticsearch"],
                            "projects": ["project1", "project2"],
                        }
                    },
                    {"_source": {"id": 2, "name": "Jane", "skills": ["java", "mongodb"], "projects": ["project3"]}},
                ]
            }
        }
        mock_connect.return_value = mock_client

        response = self.handler._use_search_api_fallback("test_index", limit=10)

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        df = response.data_frame

        # Check that arrays were converted to JSON strings
        if "skills" in df.columns:
            skills_values = df["skills"].tolist()
            for skill_value in skills_values:
                if skill_value is not None:
                    self.assertIsInstance(skill_value, str)
                    # Should be valid JSON
                    parsed = json.loads(skill_value)
                    self.assertIsInstance(parsed, list)

    def test_array_conversion_unicode_support(self):
        """Test array conversion with Unicode characters."""
        test_data = {
            "unicode_array": ["Hello 世界", "Café ñoño", "🚀 rocket"],
            "mixed_unicode": ["ASCII", "Ñandú", "🎉", 123],
        }

        result = self.handler._convert_arrays_to_strings(test_data)

        # Should preserve Unicode characters
        unicode_result = json.loads(result["unicode_array"])
        self.assertIn("Hello 世界", unicode_result)
        self.assertIn("Café ñoño", unicode_result)
        self.assertIn("🚀 rocket", unicode_result)

        mixed_result = json.loads(result["mixed_unicode"])
        self.assertIn("Ñandú", mixed_result)
        self.assertIn("🎉", mixed_result)
        self.assertEqual(mixed_result[3], 123)

    def test_array_conversion_error_handling(self):
        """Test array conversion handles non-serializable objects gracefully."""

        class NonSerializable:
            pass

        test_data = {"problematic_array": ["good", NonSerializable(), "also_good"]}

        # Should not raise exception, should convert to string representation
        result = self.handler._convert_arrays_to_strings(test_data)

        self.assertIsInstance(result["problematic_array"], str)
        # Should contain some string representation
        self.assertIn("good", result["problematic_array"])

    def test_performance_with_large_arrays(self):
        """Test array conversion performance with large arrays (optimized)."""
        import time

        # Create a large array
        large_array = list(range(1000))
        test_data = {"large_array": large_array, "regular_field": "value"}

        start_time = time.time()
        result = self.handler._convert_arrays_to_strings(test_data)
        end_time = time.time()

        # Should successfully convert large array efficiently
        self.assertIsInstance(result["large_array"], str)
        parsed = json.loads(result["large_array"])
        self.assertEqual(len(parsed), 1000)
        self.assertEqual(parsed[0], 0)
        self.assertEqual(parsed[999], 999)

        # Performance assertion - should complete within reasonable time
        self.assertLess(end_time - start_time, 1.0, "Large array conversion should be efficient")


if __name__ == "__main__":
    unittest.main()
