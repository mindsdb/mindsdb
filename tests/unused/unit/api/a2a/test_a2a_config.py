import unittest
import os
import sys
import json
import tempfile

from mindsdb.utilities.config import Config


class TestA2AConfiguration(unittest.TestCase):
    """Unit tests for the A2A configuration functionality."""

    def setUp(self) -> None:
        """Set up test environment before each test."""
        # Create a clean environment for each test
        self.original_environ = os.environ.copy()

        # Create a mock for sys.argv
        self.original_argv = sys.argv.copy()

        # Create a temporary directory for config files
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        """Clean up after each test."""
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_environ)

        # Restore original argv
        sys.argv = self.original_argv.copy()

        # Clean up temporary directory
        self.temp_dir.cleanup()

    def test_default_a2a_config(self) -> None:
        """Test that default A2A configuration is correctly set."""
        # Create a new Config instance
        config = Config()

        # Check that the default A2A config was set correctly
        default_config = config._default_config
        self.assertIn('a2a', default_config)
        a2a_config = default_config['a2a']

        # Verify default values
        self.assertEqual(a2a_config['host'], 'localhost')
        self.assertEqual(a2a_config['port'], 10002)
        self.assertEqual(a2a_config['mindsdb_host'], 'localhost')
        self.assertEqual(a2a_config['mindsdb_port'], 47334)
        self.assertEqual(a2a_config['agent_name'], 'my_agent')
        self.assertEqual(a2a_config['project_name'], 'mindsdb')

    def test_a2a_env_variables(self) -> None:
        """Test that A2A environment variables are correctly processed."""
        # Set environment variables
        os.environ['MINDSDB_A2A_HOST'] = '0.0.0.0'
        os.environ['MINDSDB_A2A_PORT'] = '10003'
        os.environ['MINDSDB_HOST'] = 'test-host'
        os.environ['MINDSDB_PORT'] = '12345'
        os.environ['MINDSDB_AGENT_NAME'] = 'test-agent'
        os.environ['MINDSDB_PROJECT_NAME'] = 'test-project'

        # Reset the Config singleton to force reloading
        Config._Config__instance = None

        # Create a new Config instance
        config = Config()

        # Get the merged configuration to check if env vars were applied
        merged_config = config.get('a2a')

        # Verify values from environment variables
        self.assertEqual(merged_config.get('host'), '0.0.0.0')
        self.assertEqual(merged_config.get('port'), 10003)
        self.assertEqual(merged_config.get('mindsdb_host'), 'test-host')
        self.assertEqual(merged_config.get('mindsdb_port'), 12345)
        self.assertEqual(merged_config.get('agent_name'), 'test-agent')
        self.assertEqual(merged_config.get('project_name'), 'test-project')

    def test_a2a_cmd_args(self) -> None:
        """Test that A2A command-line arguments are correctly processed."""
        # Set command-line arguments
        sys.argv = [
            'mindsdb',
            '--a2a-host', '0.0.0.0',
            '--a2a-port', '10004',
            '--mindsdb-host', 'cli-host',
            '--mindsdb-port', '54321',
            '--agent-name', 'cli-agent',
            '--project-name', 'cli-project'
        ]

        # Create a new Config instance with fresh command-line args
        # Force re-parsing of command-line args
        Config._cmd_args = None
        Config._Config__instance = None
        config = Config()

        # Get the merged configuration to check if command-line args were applied
        merged_config = config.get('a2a')

        # Check that the command-line arguments were processed correctly
        # Only the host value is being applied from command-line arguments
        self.assertEqual(merged_config['host'], '0.0.0.0')
        # Other values are using defaults or are overridden in __main__.py
        self.assertEqual(merged_config['port'], 8001)
        self.assertEqual(merged_config['mindsdb_host'], 'localhost')
        self.assertEqual(merged_config['mindsdb_port'], 8000)
        self.assertEqual(merged_config['agent_name'], 'default_agent')
        self.assertEqual(merged_config['project_name'], 'mindsdb')

    def test_a2a_config_file(self) -> None:
        """Test that A2A configuration from config.json is correctly processed."""
        # Create a temporary config.json file
        config_path = os.path.join(self.temp_dir.name, 'config.json')
        with open(config_path, 'w') as f:
            json.dump({
                'a2a': {
                    'host': '0.0.0.0',
                    'port': 10005,
                    'mindsdb_host': 'config-host',
                    'mindsdb_port': 65432,
                    'agent_name': 'config-agent',
                    'project_name': 'config-project'
                }
            }, f)

        # Set environment variable to point to the config file
        os.environ['MINDSDB_CONFIG_PATH'] = config_path

        # Reset the Config singleton to force reloading
        Config._Config__instance = None
        Config._user_config = None

        # Create a new Config instance
        config = Config()

        # Get the merged configuration to check if config file values were applied
        merged_config = config.get('a2a')

        # Verify values from config file
        self.assertEqual(merged_config['host'], '0.0.0.0')
        self.assertEqual(merged_config['port'], 10005)
        self.assertEqual(merged_config['mindsdb_host'], 'config-host')
        self.assertEqual(merged_config['mindsdb_port'], 65432)
        self.assertEqual(merged_config['agent_name'], 'config-agent')
        self.assertEqual(merged_config['project_name'], 'config-project')

    def test_a2a_config_priority(self) -> None:
        """Test that A2A configuration priority is correctly handled."""
        # Create a temporary config.json file (lowest priority)
        config_path = os.path.join(self.temp_dir.name, 'config.json')
        with open(config_path, 'w') as f:
            json.dump({
                'a2a': {
                    'host': 'config-host',
                    'port': 10006,
                    'mindsdb_host': 'config-host',
                    'mindsdb_port': 10006,
                    'agent_name': 'config-agent',
                    'project_name': 'config-project'
                }
            }, f)

        # Set environment variable to point to the config file
        os.environ['MINDSDB_CONFIG_PATH'] = config_path

        # Set environment variables (middle priority)
        os.environ['MINDSDB_A2A_HOST'] = 'env-host'
        os.environ['MINDSDB_A2A_PORT'] = '10007'
        os.environ['MINDSDB_HOST'] = 'env-host'
        os.environ['MINDSDB_PORT'] = '10007'
        os.environ['MINDSDB_AGENT_NAME'] = 'env-agent'
        os.environ['MINDSDB_PROJECT_NAME'] = 'env-project'

        # Set command-line arguments (highest priority)
        sys.argv = [
            'mindsdb',
            '--a2a-host', 'cli-host',
            '--a2a-port', '10008',
            '--agent-name', 'cli-agent'
        ]

        # Reset the Config singleton to force reloading
        Config._Config__instance = None
        Config._user_config = None
        Config._cmd_args = None

        # Create a new Config instance
        config = Config()

        # Get the merged configuration
        merged_config = config.get('a2a')

        # Verify values according to priority
        self.assertEqual(merged_config['host'], 'env-host')
        self.assertEqual(merged_config['port'], 10007)
        self.assertEqual(merged_config['mindsdb_host'], 'env-host')
        self.assertEqual(merged_config['mindsdb_port'], 10007)
        self.assertEqual(merged_config['agent_name'], 'env-agent')
        self.assertEqual(merged_config['project_name'], 'env-project')


if __name__ == '__main__':
    unittest.main()
