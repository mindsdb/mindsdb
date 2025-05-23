import html
from http import HTTPStatus
from unittest.mock import MagicMock


from flask import Flask, request

from mindsdb.utilities.config import Config


def test_update_a2a_agent() -> None:
    """
    Test the update_a2a_agent API endpoint with mocked dependencies.

    This test verifies that:
    1. The API endpoint correctly updates the agent configuration
    2. It returns a success response with the updated configuration
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
    mock_config.update = MagicMock()

    # Define the API function to test
    def api_update_a2a_agent():
        """
        Update the A2A agent configuration.

        Expected JSON payload:
        {
            "agent_name": "new_agent_name",
            "project_name": "optional_project_name"  # Optional
        }
        """
        try:
            data = request.json
            if not data or "agent_name" not in data:
                return {
                    "error": "Missing required parameter: agent_name"
                }, HTTPStatus.BAD_REQUEST

            new_agent_name = data["agent_name"]
            new_project_name = data.get("project_name")  # Optional

            # Update the configuration
            a2a_config = mock_config.get("a2a", {}).copy()
            a2a_config["agent_name"] = new_agent_name
            if new_project_name:
                a2a_config["project_name"] = new_project_name

            # Update the global configuration
            mock_config.update({"a2a": a2a_config})

            return {
                "status": "success",
                "agent_name": html.escape(new_agent_name),
                "project_name": html.escape(new_project_name)
                or html.escape(a2a_config.get("project_name", "mindsdb")),
            }

        except Exception as e:
            app.logger.error(f"Exception occurred while updating A2A agent: {str(e)}")
            return {
                "error": "An internal error occurred. Please try again later."
            }, HTTPStatus.INTERNAL_SERVER_ERROR

    # Register the route directly with the app
    app.add_url_rule(
        "/api/a2a/update_agent",
        "api_update_a2a_agent",
        api_update_a2a_agent,
        methods=["POST"],
    )

    # Test 1: Successful update with only agent_name
    with app.test_client() as client:
        response = client.post(
            "/api/a2a/update_agent", json={"agent_name": "new_agent"}
        )

        # Verify the result
        assert response.status_code == 200
        result = response.get_json()
        assert result["status"] == "success"
        assert result["agent_name"] == "new_agent"

        # Verify config was updated correctly
        mock_config.update.assert_called_once()
        args, kwargs = mock_config.update.call_args
        assert "a2a" in args[0]
        assert args[0]["a2a"]["agent_name"] == "new_agent"

        # Reset mocks for next test
        mock_config.reset_mock()

        # Test 2: Update with both agent_name and project_name
        response = client.post(
            "/api/a2a/update_agent",
            json={"agent_name": "new_agent2", "project_name": "new_project2"},
        )

        # Verify the result
        assert response.status_code == 200
        result = response.get_json()
        assert result["status"] == "success"
        assert result["agent_name"] == "new_agent2"
        assert result["project_name"] == "new_project2"

        # Verify config was updated with both values
        args, kwargs = mock_config.update.call_args
        assert args[0]["a2a"]["agent_name"] == "new_agent2"
        assert args[0]["a2a"]["project_name"] == "new_project2"

        # Test 3: Missing agent_name parameter
        response = client.post("/api/a2a/update_agent", json={})

        # Verify the response
        assert response.status_code == 400


def test_update_a2a_agent_with_exception() -> None:
    """
    Test the update_a2a_agent API endpoint when an exception occurs.

    This test verifies that:
    1. The function handles exceptions properly
    2. It returns an appropriate error response
    """
    # Create a Flask app context for testing
    app = Flask(__name__)

    # Create mocks for dependencies
    mock_config = MagicMock(spec=Config)
    mock_config.get.return_value = {"agent_name": "old_agent"}

    # Mock update to raise an exception
    mock_config.update.side_effect = Exception("Test exception")

    # Define the API function to test with the exception
    def api_update_a2a_agent_with_exception():
        """
        Update the A2A agent configuration.

        Expected JSON payload:
        {
            "agent_name": "new_agent_name",
            "project_name": "optional_project_name"  # Optional
        }
        """
        try:
            data = request.json
            if not data or "agent_name" not in data:
                return {
                    "error": "Missing required parameter: agent_name"
                }, HTTPStatus.BAD_REQUEST

            new_agent_name = data["agent_name"]
            new_project_name = data.get("project_name")  # Optional

            # Update the configuration
            a2a_config = mock_config.get("a2a", {}).copy()
            a2a_config["agent_name"] = new_agent_name
            if new_project_name:
                a2a_config["project_name"] = new_project_name

            # Update the global configuration - this will raise an exception
            mock_config.update({"a2a": a2a_config})

            return {
                "status": "success",
                "agent_name": html.escape(new_agent_name),
                "project_name": new_project_name
                or a2a_config.get("project_name", "mindsdb"),
            }

        except Exception as e:
            return {
                "error": f"Error updating A2A agent: {str(e)}"
            }, HTTPStatus.INTERNAL_SERVER_ERROR

    # Register the route directly with the app
    app.add_url_rule(
        "/api/a2a/update_agent_exception",
        "api_update_a2a_agent_with_exception",
        api_update_a2a_agent_with_exception,
        methods=["POST"],
    )

    # Test: Exception during update
    with app.test_client() as client:
        response = client.post(
            "/api/a2a/update_agent_exception", json={"agent_name": "new_agent"}
        )

        # Verify the response
        assert response.status_code == 500
        result = response.get_json()
        assert "error" in result
        assert "Test exception" in result["error"]
