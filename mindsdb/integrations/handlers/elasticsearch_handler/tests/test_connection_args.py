import unittest
from mindsdb.integrations.handlers.elasticsearch_handler.connection_args import connection_args, connection_args_example
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class TestConnectionArgs(unittest.TestCase):
    """
    Optimized unit tests for connection_args.py to ensure all required connection
    parameters are properly defined according to MindsDB handler specifications.
    """

    @classmethod
    def setUpClass(cls):
        """Set up shared test data for efficiency."""
        cls.valid_types = [ARG_TYPE.STR, ARG_TYPE.INT, ARG_TYPE.BOOL, ARG_TYPE.PWD]
        cls.required_params = ["hosts"]
        cls.optional_params = [
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
        cls.secret_params = ["password", "api_key"]
        cls.boolean_params = ["verify_certs"]
        cls.integer_params = ["timeout"]
        cls.ssl_params = ["ca_certs", "client_cert", "client_key", "verify_certs"]

    def test_connection_args_structure(self):
        """Test that connection_args has the correct structure."""
        self.assertIsInstance(connection_args, dict, "connection_args should be a dictionary")
        self.assertGreater(len(connection_args), 0, "connection_args should not be empty")

    def test_required_connection_parameters(self):
        """Test that all required connection parameters are present."""
        for param in self.required_params:
            self.assertIn(param, connection_args, f"Required parameter '{param}' missing from connection_args")

    def test_optional_connection_parameters(self):
        """Test that all expected optional connection parameters are present."""
        for param in self.optional_params:
            self.assertIn(param, connection_args, f"Expected parameter '{param}' missing from connection_args")

    def test_parameter_type_definitions(self):
        """Test that all parameters have valid type definitions."""
        for param_name, param_config in connection_args.items():
            self.assertIn("type", param_config, f"Parameter '{param_name}' missing 'type' field")
            self.assertIn(
                param_config["type"],
                self.valid_types,
                f"Parameter '{param_name}' has invalid type: {param_config['type']}",
            )

    def test_parameter_descriptions(self):
        """Test that all parameters have descriptions."""
        for param_name, param_config in connection_args.items():
            self.assertIn("description", param_config, f"Parameter '{param_name}' missing 'description' field")
            self.assertIsInstance(
                param_config["description"], str, f"Parameter '{param_name}' description should be a string"
            )
            self.assertGreater(
                len(param_config["description"]), 0, f"Parameter '{param_name}' description should not be empty"
            )

    def test_secret_parameters(self):
        """Test that sensitive parameters are marked as secret."""
        for param in self.secret_params:
            if param in connection_args:
                self.assertIn(
                    "secret", connection_args[param], f"Secret parameter '{param}' should have 'secret' field"
                )
                self.assertTrue(connection_args[param]["secret"], f"Secret parameter '{param}' should have secret=True")

    def test_password_parameter_type(self):
        """Test that password parameter uses PWD type."""
        if "password" in connection_args:
            self.assertEqual(
                connection_args["password"]["type"], ARG_TYPE.PWD, "Password parameter should use ARG_TYPE.PWD"
            )

    def test_boolean_parameter_types(self):
        """Test that boolean parameters use correct type."""
        for param in self.boolean_params:
            if param in connection_args:
                self.assertEqual(
                    connection_args[param]["type"],
                    ARG_TYPE.BOOL,
                    f"Boolean parameter '{param}' should use ARG_TYPE.BOOL",
                )

    def test_integer_parameter_types(self):
        """Test that integer parameters use correct type."""
        for param in self.integer_params:
            if param in connection_args:
                self.assertEqual(
                    connection_args[param]["type"], ARG_TYPE.INT, f"Integer parameter '{param}' should use ARG_TYPE.INT"
                )

    def test_connection_args_example_structure(self):
        """Test that connection_args_example has correct structure."""
        self.assertIsInstance(connection_args_example, dict, "connection_args_example should be a dictionary")
        self.assertGreater(len(connection_args_example), 0, "connection_args_example should not be empty")

    def test_connection_args_example_has_required_params(self):
        """Test that connection_args_example includes required parameters."""
        required_params = ["hosts"]

        for param in required_params:
            self.assertIn(
                param, connection_args_example, f"connection_args_example missing required parameter: {param}"
            )

    def test_connection_args_example_values(self):
        """Test that connection_args_example has valid example values."""
        if "hosts" in connection_args_example:
            self.assertIsInstance(connection_args_example["hosts"], str, "hosts example should be a string")
            self.assertGreater(len(connection_args_example["hosts"]), 0, "hosts example should not be empty")

    def test_hosts_parameter_description(self):
        """Test that hosts parameter has comprehensive description."""
        hosts_config = connection_args["hosts"]
        description = hosts_config["description"]

        # Check that description mentions multiple hosts and format
        self.assertIn("host", description.lower(), "hosts description should mention 'host'")
        self.assertIn(
            "comma", description.lower(), "hosts description should mention comma separation for multiple hosts"
        )

    def test_cloud_id_parameter_description(self):
        """Test that cloud_id parameter has appropriate description."""
        if "cloud_id" in connection_args:
            cloud_id_config = connection_args["cloud_id"]
            description = cloud_id_config["description"]

            # Check for relevant terms (the description uses "hosted" instead of "cloud")
            relevant_terms = ["hosted", "elasticsearch", "service"]
            has_relevant_term = any(term in description.lower() for term in relevant_terms)
            self.assertTrue(has_relevant_term, "cloud_id description should mention hosted/elasticsearch/service")
            self.assertIn("elasticsearch", description.lower(), "cloud_id description should mention 'Elasticsearch'")

    def test_ssl_parameters_present(self):
        """Test that SSL/TLS related parameters are present."""
        for param in self.ssl_params:
            self.assertIn(param, connection_args, f"SSL parameter '{param}' should be present for enterprise security")

    def test_parameter_consistency(self):
        """Test consistency between connection_args and connection_args_example."""
        # All parameters in example should exist in connection_args
        for param in connection_args_example:
            self.assertIn(param, connection_args, f"Parameter '{param}' in example but not in connection_args")

    def test_comprehensive_coverage(self):
        """Test that we have comprehensive parameter coverage for Elasticsearch."""
        expected_categories = {
            "connection": ["hosts", "cloud_id"],
            "authentication": ["user", "password", "api_key"],
            "security": ["ca_certs", "client_cert", "client_key", "verify_certs"],
            "performance": ["timeout"],
        }

        all_expected_params = set()
        for category_params in expected_categories.values():
            all_expected_params.update(category_params)

        actual_params = set(connection_args.keys())

        # Check that we have all expected parameters
        missing_params = all_expected_params - actual_params
        self.assertEqual(len(missing_params), 0, f"Missing expected parameters: {missing_params}")


if __name__ == "__main__":
    unittest.main()
