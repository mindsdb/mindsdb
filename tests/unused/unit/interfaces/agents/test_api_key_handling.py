import os
import unittest
from unittest.mock import patch, MagicMock

from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.interfaces.agents.agents_controller import AgentsController


class TestAgentApiKeyHandling(unittest.TestCase):
    """Test API key handling in agent creation and usage."""

    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'OPENAI_API_KEY': 'test-env-api-key',
            'ANTHROPIC_API_KEY': 'test-env-anthropic-key'
        })
        self.env_patcher.start()

    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()

    def test_get_api_key_from_env(self):
        """Test retrieving API key from environment variables."""
        # Test getting API key from environment variable
        api_key = get_api_key('openai', {})
        self.assertEqual(api_key, 'test-env-api-key')

    def test_get_api_key_from_args(self):
        """Test retrieving API key from create_args."""
        # Test getting API key from create_args
        api_key = get_api_key('openai', {'openai_api_key': 'test-args-api-key'})
        self.assertEqual(api_key, 'test-args-api-key')

    def test_get_api_key_from_params(self):
        """Test retrieving API key from params dictionary."""
        # Test getting API key from params dictionary
        api_key = get_api_key('openai', {'params': {'openai_api_key': 'test-params-api-key'}})
        self.assertEqual(api_key, 'test-params-api-key')

    def test_get_api_key_priority(self):
        """Test API key retrieval priority."""
        # Test that create_args takes priority over environment variables
        api_key = get_api_key('openai', {'openai_api_key': 'test-args-api-key'})
        self.assertEqual(api_key, 'test-args-api-key')

        # Test that params takes priority over environment variables
        api_key = get_api_key('openai', {'params': {'openai_api_key': 'test-params-api-key'}})
        self.assertEqual(api_key, 'test-params-api-key')

        # Test that create_args takes priority over params
        api_key = get_api_key('openai', {
            'openai_api_key': 'test-args-api-key',
            'params': {'openai_api_key': 'test-params-api-key'}
        })
        self.assertEqual(api_key, 'test-args-api-key')

    @patch('mindsdb.interfaces.agents.agents_controller.AgentsController.check_model_provider')
    @patch('mindsdb.interfaces.agents.agents_controller.AgentsController.get_agent')
    @patch('mindsdb.interfaces.agents.agents_controller.ProjectController')
    @patch('mindsdb.interfaces.storage.db.session')
    def test_add_agent_with_api_key(self, mock_session, mock_project_controller, mock_get_agent, mock_check_model_provider):
        """Test adding an agent with an API key in params."""
        # Mock project controller
        mock_project = MagicMock()
        mock_project_controller.return_value.get.return_value = mock_project

        # Mock get_agent to return None (agent doesn't exist yet)
        mock_get_agent.return_value = None

        # Mock check_model_provider to return a provider
        mock_check_model_provider.return_value = (None, 'openai')

        # Create an instance of AgentsController
        agent_controller = AgentsController()

        # Test adding an agent with an API key in params
        params = {
            'openai_api_key': 'test-agent-api-key',
            'other_param': 'value'
        }

        # Create a mock agent with proper params
        mock_agent = MagicMock()
        mock_agent.params = params.copy()  # Set params directly

        # Mock db.Agents to return our prepared mock agent
        with patch('mindsdb.interfaces.storage.db.Agents', return_value=mock_agent):
            # Add the agent
            agent = agent_controller.add_agent(
                name='test_agent',
                project_name='mindsdb',
                model_name='gpt-4',
                skills=[],
                provider='openai',
                params=params
            )

        # Verify that the API key was preserved in the params
        self.assertEqual(agent.params.get('openai_api_key'), 'test-agent-api-key')


if __name__ == '__main__':
    unittest.main()
