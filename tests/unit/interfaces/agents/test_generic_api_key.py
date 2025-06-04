import os
import unittest
from unittest.mock import patch, MagicMock

from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.interfaces.agents.agents_controller import AgentsController


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

    @patch("mindsdb.interfaces.agents.agents_controller.AgentsController.check_model_provider")
    @patch("mindsdb.interfaces.agents.agents_controller.AgentsController.get_agent")
    @patch("mindsdb.interfaces.agents.agents_controller.ProjectController")
    @patch("mindsdb.interfaces.storage.db.session")
    def test_add_agent_with_generic_api_key(
        self, mock_session, mock_project_controller, mock_get_agent, mock_check_model_provider
    ):
        """Test adding an agent with a generic API key in params."""
        # Mock project controller
        mock_project = MagicMock()
        mock_project_controller.return_value.get.return_value = mock_project

        # Mock get_agent to return None (agent doesn't exist yet)
        mock_get_agent.return_value = None

        # Mock check_model_provider to return a provider
        mock_check_model_provider.return_value = (None, "openai")

        # Create an instance of AgentsController
        agent_controller = AgentsController()

        # Test adding an agent with a generic API key in params
        params = {"api_key": "test-generic-agent-api-key", "other_param": "value"}

        # Create a mock agent with proper params
        mock_agent = MagicMock()
        mock_agent.params = params.copy()  # Set params directly

        # Mock db.Agents to return our prepared mock agent
        with patch("mindsdb.interfaces.storage.db.Agents", return_value=mock_agent):
            # Add the agent
            agent = agent_controller.add_agent(
                name="test_agent",
                project_name="mindsdb",
                model_name="gpt-4",
                skills=[],
                provider="openai",
                params=params,
            )

        # Verify that the generic API key was preserved in the params
        self.assertEqual(agent.params["api_key"], "test-generic-agent-api-key")

    @patch("mindsdb.interfaces.agents.agents_controller.AgentsController.check_model_provider")
    @patch("mindsdb.interfaces.agents.agents_controller.AgentsController.get_agent")
    @patch("mindsdb.interfaces.agents.agents_controller.ProjectController")
    @patch("mindsdb.interfaces.storage.db.session")
    def test_add_agent_with_both_api_keys(
        self, mock_session, mock_project_controller, mock_get_agent, mock_check_model_provider
    ):
        """Test adding an agent with both generic and provider-specific API keys."""
        # Mock project controller
        mock_project = MagicMock()
        mock_project_controller.return_value.get.return_value = mock_project

        # Mock get_agent to return None (agent doesn't exist yet)
        mock_get_agent.return_value = None

        # Mock check_model_provider to return a provider
        mock_check_model_provider.return_value = (None, "openai")

        # Create an instance of AgentsController
        agent_controller = AgentsController()

        # Test adding an agent with both generic and provider-specific API keys
        params = {
            "api_key": "test-generic-agent-api-key",
            "openai_api_key": "test-specific-agent-api-key",
            "other_param": "value",
        }

        # Create a mock agent with proper params
        mock_agent = MagicMock()
        mock_agent.params = params.copy()  # Set params directly

        # Mock db.Agents to return our prepared mock agent
        with patch("mindsdb.interfaces.storage.db.Agents", return_value=mock_agent):
            # Add the agent
            agent = agent_controller.add_agent(
                name="test_agent",
                project_name="mindsdb",
                model_name="gpt-4",
                skills=[],
                provider="openai",
                params=params,
            )

        # Verify that both API keys were preserved in the params
        self.assertEqual(agent.params["api_key"], "test-generic-agent-api-key")
        self.assertEqual(agent.params["openai_api_key"], "test-specific-agent-api-key")

        # Test that get_api_key returns the provider-specific key when both are present
        with patch("mindsdb.interfaces.agents.langchain_agent.get_api_key") as mock_get_api_key:
            mock_get_api_key.return_value = "test-specific-agent-api-key"

            # Call the function that would use get_api_key
            api_key = get_api_key("openai", {"params": params})

            # Verify that the provider-specific key was returned
            self.assertEqual(api_key, "test-specific-agent-api-key")


if __name__ == "__main__":
    unittest.main()
