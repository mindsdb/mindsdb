import os
import unittest
import pytest

import pandas as pd

from mindsdb.interfaces.agents.pydantic_agent import PydanticAgent
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.agents.constants import ASSISTANT_COLUMN, CONTEXT_COLUMN, TRACE_ID_COLUMN


@pytest.mark.skipif(
    os.environ.get("OPENAI_API_KEY") is None,
    reason="OPENAI_API_KEY environment variable not set"
)
class TestPydanticAgent(unittest.TestCase):
    """Integration tests for the PydanticAgent class with actual OpenAI API calls"""

    def setUp(self):
        """Set up a db.Agents instance for testing"""
        self.agent = db.Agents()
        self.agent.model_name = "gpt-4o"
        self.agent.provider = "openai"
        self.agent.params = {
            "OPENAI_API_KEY": os.environ.get("OPENAI_API_KEY"),
            "prompt_template": "You are a helpful assistant that answers questions concisely."
        }
        self.agent.skills_relationships = []

    def test_regular_completion(self):
        """Test regular (non-streaming) completion with PydanticAgent"""
        # Create a PydanticAgent with the real db.Agents instance
        pydantic_agent = PydanticAgent(self.agent)

        # Sample messages
        messages = [{"content": "What is the capital of France?"}]

        # Get completion
        result = pydantic_agent.get_completion(messages)

        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn(ASSISTANT_COLUMN, result.columns)
        self.assertIn(CONTEXT_COLUMN, result.columns)
        self.assertIn(TRACE_ID_COLUMN, result.columns)

        # Check for expected content in the response
        response_text = result[ASSISTANT_COLUMN][0]
        self.assertIn("Paris", response_text, f"Expected 'Paris' in response: {response_text}")

    def test_streaming_completion(self):
        """Test streaming completion with PydanticAgent"""
        # Create a PydanticAgent with the real db.Agents instance
        pydantic_agent = PydanticAgent(self.agent)

        # Sample messages
        messages = [{"content": "What is the capital of France?"}]

        # Get streaming completion
        chunks = []
        for chunk in pydantic_agent.get_completion(messages, stream=True):
            chunks.append(chunk)

        # Verify chunks
        self.assertTrue(len(chunks) > 2, f"Expected multiple chunks, got {len(chunks)}")

        # First chunk should be start
        self.assertEqual(chunks[0]["type"], "start")

        # Check for content chunks
        content_chunks = [c for c in chunks if c.get("type") == "chunk"]
        self.assertTrue(len(content_chunks) > 0, "No content chunks found")

        # Concatenate content to verify full response
        content = ""
        for chunk in content_chunks:
            if "content" in chunk:
                content += chunk["content"]

        # Check that Paris is in the concatenated response
        self.assertIn("Paris", content, f"Expected 'Paris' in the streamed content, got: {content}")

        # Last chunk should be end
        self.assertEqual(chunks[-1]["type"], "end")


if __name__ == "__main__":
    unittest.main()
