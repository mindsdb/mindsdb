import os
import unittest
from unittest.mock import patch

from mindsdb.integrations.utilities.handler_utils import get_api_key


class TestGenericApiKeyHandling(unittest.TestCase):
    """Test generic API key handling in agent creation and usage."""

    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.env_patcher = patch.dict(
            os.environ, {"OPENAI_API_KEY": "test-env-api-key", "ANTHROPIC_API_KEY": "test-env-anthropic-key"}
        )
        self.env_patcher.start()

    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()

    def test_get_generic_api_key_from_args(self):
        """Test retrieving generic API key from create_args."""
        # Test getting generic API key from create_args
        api_key = get_api_key("openai", {"api_key": "test-generic-api-key"})
        self.assertEqual(api_key, "test-generic-api-key")

    def test_get_generic_api_key_from_params(self):
        """Test retrieving generic API key from params dictionary."""
        # Test getting generic API key from params dictionary
        api_key = get_api_key("openai", {"params": {"api_key": "test-generic-params-api-key"}})
        self.assertEqual(api_key, "test-generic-params-api-key")

    def test_get_generic_api_key_from_using(self):
        """Test retrieving generic API key from using dictionary."""
        # Test getting generic API key from using dictionary
        api_key = get_api_key("openai", {"using": {"api_key": "test-generic-using-api-key"}})
        self.assertEqual(api_key, "test-generic-using-api-key")

    def test_provider_specific_key_priority_over_generic(self):
        """Test that provider-specific keys take priority over generic keys."""
        # Test that provider-specific key takes priority over generic key in args
        api_key = get_api_key("openai", {"openai_api_key": "test-specific-api-key", "api_key": "test-generic-api-key"})
        self.assertEqual(api_key, "test-specific-api-key")

        # Test that provider-specific key takes priority over generic key in params
        api_key = get_api_key(
            "openai",
            {"params": {"openai_api_key": "test-specific-params-api-key", "api_key": "test-generic-params-api-key"}},
        )
        self.assertEqual(api_key, "test-specific-params-api-key")

        # Test that provider-specific key takes priority over generic key in using
        api_key = get_api_key(
            "openai",
            {"using": {"openai_api_key": "test-specific-using-api-key", "api_key": "test-generic-using-api-key"}},
        )
        self.assertEqual(api_key, "test-specific-using-api-key")

    def test_get_generic_api_key_for_google_provider(self):
        """Test retrieving generic API key for Google/Gemini provider."""
        # Test getting generic API key for Google provider
        api_key = get_api_key("google", {"api_key": "test-generic-google-api-key"})
        self.assertEqual(api_key, "test-generic-google-api-key")

        # Test that provider-specific key takes priority for Google provider
        api_key = get_api_key(
            "google", {"google_api_key": "test-specific-google-api-key", "api_key": "test-generic-google-api-key"}
        )
        self.assertEqual(api_key, "test-specific-google-api-key")


if __name__ == "__main__":
    unittest.main()
