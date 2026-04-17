"""
Unit tests for MCP prompts (mindsdb/api/mcp/prompts/*).

mcp.get_prompt() is async; tests run it with asyncio.run().
"""

import json
import asyncio

from mindsdb.api.mcp.mcp_instance import mcp


def _run(coro):
    return asyncio.run(coro)


def _get_sample_table_prompt(database_name: str, table_name: str):
    """Call sample_table prompt and return the GetPromptResult."""
    return _run(mcp.get_prompt("sample_table", {"database_name": database_name, "table_name": table_name}))


def _get_first_message_text(prompt: object) -> str:
    """Return the text content of the first message."""
    raw = prompt.messages[0].content.text
    # FastMCP serialises the TextContent to JSON inside the PromptMessage
    return json.loads(raw)["text"]


class TestPrompt:
    def test_sample_table_exists(self):
        # sample_table exists and has description
        prompts = _run(mcp.list_prompts())
        prompt = next(p for p in prompts if p.name == "sample_table")
        assert prompt.description  # non-empty

    def test_sample_table_content(self):
        # test content of the prompt
        result = _get_sample_table_prompt("MyDB", "mytable")
        assert len(result.messages) == 1
        assert result.messages[0].role == "user"
        assert result.messages[0].content.type == "text"

        text = _get_first_message_text(result)
        assert "`MyDB`.`mytable`" in text
        assert "limit 5" in text.lower()
