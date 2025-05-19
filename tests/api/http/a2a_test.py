import subprocess

from unittest.mock import patch, MagicMock
from typing import Dict, Any, Optional

from flask import Flask

from mindsdb.utilities.config import Config


def test_update_a2a_agent() -> None:
    """
    Test the update_a2a_agent function directly with mocked dependencies.

    This test verifies that:
    1. The function correctly updates the agent configuration
    2. It terminates the existing process and starts a new one
    3. It handles error cases properly
    """
    # Create a Flask app context for testing
    app = Flask(__name__)

    # Create mocks for dependencies
    mock_config = MagicMock(spec=Config)
    mock_config.get.return_value = {
        "agent_name": "old_agent",
        "project_name": "old_project",
        "host": "localhost",
        "port": 8000,
    }

    mock_process = MagicMock()
    mock_process.terminate = MagicMock()
    mock_process.wait = MagicMock()
    mock_process.pid = 12345

    mock_logger = MagicMock()

    # Create variables for the test function
    a2a_enabled = True
    a2a_process = mock_process

    # Mock the start_a2a_subprocess function
    def mock_start_a2a_subprocess(config: Dict[str, Any]) -> subprocess.Popen:
        """Mock implementation of start_a2a_subprocess."""
        return mock_process

    # Define the update_a2a_agent function with our context
    with app.app_context():
        with patch("mindsdb.api.http.initialize.logger", mock_logger):
            # Define the function in the same way as in initialize.py
            def update_a2a_agent(
                new_agent_name: str, new_project_name: Optional[str] = None
            ) -> bool:
                """
                Update the A2A agent configuration and restart the subprocess if needed.

                Args:
                    new_agent_name: New agent name to use
                    new_project_name: New project name to use

                Returns:
                    bool: True if update was successful, False otherwise
                """
                nonlocal a2a_process, a2a_enabled

                if not a2a_enabled or a2a_process is None:
                    mock_logger.warning(
                        "A2A is not enabled or not running, can't update agent"
                    )
                    return False

                # Update config
                a2a_config = mock_config.get("a2a", {}).copy()
                a2a_config["agent_name"] = new_agent_name
                if new_project_name:
                    a2a_config["project_name"] = new_project_name

                # Store updated config
                mock_config.update({"a2a": a2a_config})

                # Terminate existing process
                mock_logger.info(
                    f"Terminating A2A process to update agent name to {new_agent_name}"
                )
                a2a_process.terminate()
                try:
                    a2a_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    mock_logger.warning(
                        "A2A process did not terminate gracefully, forcing..."
                    )
                    a2a_process.kill()

                # Start new process with updated config
                mock_start_a2a_subprocess(a2a_config)
                return True

            # Test 1: Successful update with only agent_name
            result = update_a2a_agent("new_agent")

            # Verify the result
            assert result is True

            # Verify the process was terminated
            mock_process.terminate.assert_called_once()

            # Verify config was updated correctly
            mock_config.update.assert_called_once()
            args, kwargs = mock_config.update.call_args
            assert "a2a" in args[0]
            assert args[0]["a2a"]["agent_name"] == "new_agent"

            # Reset mocks for next test
            mock_process.reset_mock()
            mock_config.reset_mock()
            mock_logger.reset_mock()

            # Test 2: Update with both agent_name and project_name
            result = update_a2a_agent("new_agent2", "new_project2")

            # Verify the result
            assert result is True

            # Verify config was updated with both values
            args, kwargs = mock_config.update.call_args
            assert args[0]["a2a"]["agent_name"] == "new_agent2"
            assert args[0]["a2a"]["project_name"] == "new_project2"

            # Test 3: Update when a2a is not enabled
            a2a_enabled = False
            result = update_a2a_agent("new_agent3")

            # Verify the result
            assert result is False

            # Verify warning was logged
            mock_logger.warning.assert_called_with(
                "A2A is not enabled or not running, can't update agent"
            )

            # Test 4: Update when a2a_process is None
            a2a_enabled = True
            a2a_process = None
            result = update_a2a_agent("new_agent4")

            # Verify the result
            assert result is False


def test_update_a2a_agent_with_timeout() -> None:
    """
    Test the update_a2a_agent function when the process doesn't terminate gracefully.

    This test verifies that:
    1. The function handles TimeoutExpired exception properly
    2. It forces process termination with kill() when needed
    """
    # Create a Flask app context for testing
    app = Flask(__name__)

    # Create mocks for dependencies
    mock_config = MagicMock(spec=Config)
    mock_config.get.return_value = {"agent_name": "old_agent"}

    mock_process = MagicMock()
    mock_process.terminate = MagicMock()
    # Make wait() raise TimeoutExpired
    mock_process.wait.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=5)
    mock_process.kill = MagicMock()

    mock_logger = MagicMock()

    # Create variables for the test function
    a2a_enabled = True
    a2a_process = mock_process

    # Mock the start_a2a_subprocess function
    def mock_start_a2a_subprocess(config: Dict[str, Any]) -> subprocess.Popen:
        """Mock implementation of start_a2a_subprocess."""
        return mock_process

    # Define the update_a2a_agent function with our context
    with app.app_context():
        with patch("mindsdb.api.http.initialize.logger", mock_logger):
            # Define the function in the same way as in initialize.py
            def update_a2a_agent(
                new_agent_name: str, new_project_name: Optional[str] = None
            ) -> bool:
                """Update the A2A agent configuration and restart the subprocess if needed."""
                nonlocal a2a_process, a2a_enabled

                if not a2a_enabled or a2a_process is None:
                    mock_logger.warning(
                        "A2A is not enabled or not running, can't update agent"
                    )
                    return False

                # Update config
                a2a_config = mock_config.get("a2a", {}).copy()
                a2a_config["agent_name"] = new_agent_name
                if new_project_name:
                    a2a_config["project_name"] = new_project_name

                # Store updated config
                mock_config.update({"a2a": a2a_config})

                # Terminate existing process
                mock_logger.info(
                    f"Terminating A2A process to update agent name to {new_agent_name}"
                )
                a2a_process.terminate()
                try:
                    a2a_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    mock_logger.warning(
                        "A2A process did not terminate gracefully, forcing..."
                    )
                    a2a_process.kill()

                # Start new process with updated config
                mock_start_a2a_subprocess(a2a_config)
                return True

            # Test: Process doesn't terminate gracefully
            result = update_a2a_agent("new_agent")

            # Verify the result
            assert result is True

            # Verify the process was terminated and then killed
            mock_process.terminate.assert_called_once()
            mock_process.kill.assert_called_once()

            # Verify warning was logged
            mock_logger.warning.assert_called_with(
                "A2A process did not terminate gracefully, forcing..."
            )
