#!/usr/bin/env python
"""
Unit tests for the MindsDBAgent streaming functionality.
These tests verify that the stream and streaming_invoke methods work correctly.
"""

import pytest
import requests
from unittest.mock import patch, MagicMock
from mindsdb.api.a2a.agent import MindsDBAgent


class TestMindsDBAgentStreaming:
    """Test suite for MindsDBAgent streaming functionality."""

    @pytest.fixture
    def agent(self):
        """Create a test agent instance."""
        return MindsDBAgent(
            agent_name="test_agent",
            project_name="mindsdb",
            host="localhost",
            port=47334,
        )

    @pytest.mark.asyncio
    async def test_stream(self, agent):
        """Test the stream method returns expected chunks."""
        # Mock the streaming_invoke method to return predefined chunks
        test_chunks = [
            {"type": "start", "prompt": "test query"},
            {"output": "test response", "is_task_complete": True},
        ]

        with patch.object(agent, "streaming_invoke", return_value=iter(test_chunks)):
            # Call the stream method
            chunks = []
            async for chunk in agent.stream("test query", "test_session"):
                chunks.append(chunk)

            # Verify the chunks
            assert len(chunks) == 2
            assert chunks[0]["type"] == "start"
            assert chunks[1]["output"] == "test response"
            assert chunks[1]["is_task_complete"] is True

    def test_streaming_invoke_success(self, agent):
        """Test successful streaming_invoke with mocked response."""
        # Create a mock response with SSE format data
        mock_response = MagicMock()
        mock_response.iter_lines.return_value = [
            b'data: {"type": "start", "prompt": "test query"}',
            b'data: {"output": "test response", "is_task_complete": true}',
        ]

        # Patch the requests.post to return our mock response
        with patch("requests.post", return_value=mock_response):
            # Call streaming_invoke
            messages = [{"question": "test query", "answer": None}]
            chunks = list(agent.streaming_invoke(messages))

            # Verify the chunks
            assert len(chunks) == 2
            assert chunks[0]["type"] == "start"
            assert chunks[1]["output"] == "test response"
            assert chunks[1].get("is_task_complete") is True

    def test_streaming_invoke_error_handling(self, agent):
        """Test error handling in streaming_invoke."""
        # Test timeout error
        with patch(
            "requests.post",
            side_effect=requests.exceptions.Timeout("Request timed out"),
        ):
            chunks = list(agent.streaming_invoke([{"question": "test query"}]))
            assert len(chunks) == 1
            assert "timed out" in chunks[0]["content"].lower()
            assert chunks[0]["is_task_complete"] is True
            assert chunks[0]["error"] == "timeout"

    def test_streaming_invoke_connection_error(self, agent):
        """Test handling of connection errors."""
        # Test connection error
        with patch(
            "requests.post",
            side_effect=requests.exceptions.ConnectionError("Connection refused"),
        ):
            chunks = list(agent.streaming_invoke([{"question": "test query"}]))
            assert len(chunks) == 1
            assert "connection error" in chunks[0]["content"].lower()
            assert chunks[0]["is_task_complete"] is True
            assert chunks[0]["error"] == "connection_error"

    def test_streaming_invoke_json_error(self, agent):
        """Test handling of JSON parsing errors."""
        # Create a mock response with invalid JSON
        mock_response = MagicMock()
        mock_response.iter_lines.return_value = [
            b'data: {"valid": "json"}',
            b"data: {invalid json}",
        ]

        # Patch the requests.post to return our mock response
        with patch("requests.post", return_value=mock_response):
            # Call streaming_invoke
            messages = [{"question": "test query"}]
            chunks = list(agent.streaming_invoke(messages))

            # Verify we get the valid chunk and an error for the invalid one
            assert len(chunks) == 2
            assert chunks[0]["valid"] == "json"
            assert "error" in chunks[1]
            assert "JSON parse error" in chunks[1]["error"]
            assert chunks[1]["is_task_complete"] is False


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
