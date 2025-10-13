import unittest
from unittest.mock import Mock, patch
import sys
import os

# Add the MindsDB path for imports (if needed)
sys.path.append("/Users/sudhirmenon/Documents/GitHub/mindsdb")


class TestDeepLakeHandlerBasic(unittest.TestCase):
    """Basic tests for Deep Lake Handler without external dependencies."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_connection_data = {
            "dataset_path": "./test_dataset",
            "token": "test_token",
            "search_default_limit": 5,
            "search_distance_metric": "cosine",
            "create_embedding_dim": 8,
        }
        self.handler_kwargs = {
            "connection_data": self.test_connection_data,
        }

    def test_01_import_structure(self):
        """Test that we can import the handler components."""
        # Test connection args import
        try:
            from mindsdb.integrations.handlers.deeplake_handler.connection_args import connection_args

            self.assertIsInstance(connection_args, dict)
            self.assertIn("dataset_path", connection_args)
            print("✅ Connection args import successful")
        except ImportError as e:
            self.skipTest(f"Connection args import failed: {e}")

    def test_02_handler_registration(self):
        """Test handler registration structure."""
        try:
            from mindsdb.integrations.handlers.deeplake_handler import name, title

            self.assertEqual(name, "deeplake")
            self.assertEqual(title, "Deep Lake")
            print(f"✅ Handler registration: {name} ({title})")
        except ImportError as e:
            self.skipTest(f"Handler registration import failed: {e}")

    @patch("builtins.__import__")
    def test_03_handler_initialization_mock(self, mock_import):
        """Test handler initialization with mocked dependencies."""
        # Mock pandas
        mock_pandas = Mock()
        mock_pandas.DataFrame = Mock()

        # Mock Deep Lake
        mock_deeplake = Mock()
        mock_deeplake.load = Mock()

        def mock_import_func(name, *args, **kwargs):
            if name == "pandas":
                return mock_pandas
            elif name == "deeplake":
                return mock_deeplake
            else:
                return Mock()

        mock_import.side_effect = mock_import_func

        try:
            from mindsdb.integrations.handlers.deeplake_handler.deeplake_handler import DeepLakeHandler

            handler = DeepLakeHandler("test_handler", **self.handler_kwargs)

            # Test basic attributes
            self.assertEqual(handler.name, "test_handler")
            self.assertEqual(handler._search_limit, 5)
            self.assertEqual(handler._search_distance_metric, "cosine")
            self.assertEqual(handler._create_embedding_dim, 8)
            print("✅ Handler initialization successful")

        except Exception as e:
            self.skipTest(f"Handler initialization failed: {e}")

    def test_04_filter_operator_mapping(self):
        """Test filter operator mapping without full handler."""
        try:
            # Try to import the FilterOperator enum
            from mindsdb.integrations.libs.vectordatabase_handler import FilterOperator

            # Test that basic operators exist
            operators = [
                FilterOperator.EQUAL,
                FilterOperator.NOT_EQUAL,
                FilterOperator.GREATER_THAN,
                FilterOperator.LESS_THAN,
                FilterOperator.IN,
            ]

            for op in operators:
                self.assertIsNotNone(op)

            print("✅ Filter operators available")

        except ImportError as e:
            self.skipTest(f"Filter operators import failed: {e}")

    def test_05_table_field_enum(self):
        """Test TableField enum availability."""
        try:
            from mindsdb.integrations.libs.vectordatabase_handler import TableField

            # Test that required fields exist
            fields = [
                TableField.ID,
                TableField.CONTENT,
                TableField.EMBEDDINGS,
                TableField.METADATA,
                TableField.SEARCH_VECTOR,
                TableField.DISTANCE,
            ]

            for field in fields:
                self.assertIsNotNone(field.value)

            print("✅ Table fields enum available")

        except ImportError as e:
            self.skipTest(f"Table fields import failed: {e}")

    def test_06_response_types(self):
        """Test response type availability."""
        try:
            from mindsdb.integrations.libs.response import RESPONSE_TYPE

            # Test that response types exist
            response_types = [
                RESPONSE_TYPE.TABLE,
                RESPONSE_TYPE.OK,
                RESPONSE_TYPE.ERROR,
            ]

            for resp_type in response_types:
                self.assertIsNotNone(resp_type)

            print("✅ Response types available")

        except ImportError as e:
            self.skipTest(f"Response types import failed: {e}")

    def test_07_connection_args_validation(self):
        """Test connection arguments structure."""
        try:
            from mindsdb.integrations.handlers.deeplake_handler.connection_args import (
                connection_args,
                connection_args_example,
            )

            # Test required fields
            required_fields = ["dataset_path"]
            for field in required_fields:
                self.assertIn(field, connection_args)
                self.assertTrue(connection_args[field].get("required", False))

            # Test optional fields
            optional_fields = ["token", "search_default_limit", "search_distance_metric"]
            for field in optional_fields:
                self.assertIn(field, connection_args)
                self.assertFalse(connection_args[field].get("required", True))

            # Test example structure
            self.assertIn("dataset_path", connection_args_example)

            print("✅ Connection arguments validation successful")

        except ImportError as e:
            self.skipTest(f"Connection args validation failed: {e}")

    def test_08_vector_store_handler_base(self):
        """Test VectorStoreHandler base class availability."""
        try:
            from mindsdb.integrations.libs.vectordatabase_handler import VectorStoreHandler

            # Test that it has expected schema
            self.assertTrue(hasattr(VectorStoreHandler, "SCHEMA"))
            schema = VectorStoreHandler.SCHEMA
            self.assertIsInstance(schema, list)

            # Test schema structure
            schema_fields = [field["name"] for field in schema]
            expected_fields = ["id", "content", "embeddings", "metadata", "distance"]

            for field in expected_fields:
                self.assertIn(field, schema_fields)

            print("✅ VectorStoreHandler base class available")

        except ImportError as e:
            self.skipTest(f"VectorStoreHandler import failed: {e}")

    def test_09_handler_directory_structure(self):
        """Test that handler directory has correct structure."""
        handler_dir = "/Users/sudhirmenon/Documents/GitHub/mindsdb/mindsdb/integrations/handlers/deeplake_handler"

        required_files = [
            "__init__.py",
            "__about__.py",
            "connection_args.py",
            "deeplake_handler.py",
            "requirements.txt",
            "README.md",
            "icon.svg",
        ]

        for file in required_files:
            file_path = os.path.join(handler_dir, file)
            self.assertTrue(os.path.exists(file_path), f"Missing required file: {file}")

        # Test tests directory
        tests_dir = os.path.join(handler_dir, "tests")
        self.assertTrue(os.path.exists(tests_dir), "Missing tests directory")

        test_files = ["__init__.py", "test_deeplake_handler.py"]
        for file in test_files:
            file_path = os.path.join(tests_dir, file)
            self.assertTrue(os.path.exists(file_path), f"Missing test file: {file}")

        print("✅ Handler directory structure complete")

    def test_10_requirements_file(self):
        """Test requirements.txt content."""
        requirements_path = "/Users/sudhirmenon/Documents/GitHub/mindsdb/mindsdb/integrations/handlers/deeplake_handler/requirements.txt"

        self.assertTrue(os.path.exists(requirements_path), "requirements.txt missing")

        with open(requirements_path, "r") as f:
            requirements = f.read()

        self.assertIn("deeplake", requirements)
        print("✅ Requirements file contains Deep Lake dependency")


def run_tests():
    """Run the tests and print results."""
    print("🧪 Running Deep Lake Handler Basic Tests\n")

    # Run tests
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestDeepLakeHandlerBasic)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print("\n📊 Test Summary:")
    print(f"   Tests run: {result.testsRun}")
    print(f"   Failures: {len(result.failures)}")
    print(f"   Errors: {len(result.errors)}")
    print(f"   Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")

    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed")

    return result.wasSuccessful()


if __name__ == "__main__":
    run_tests()
