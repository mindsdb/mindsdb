"""
Unit tests for the MCP completion handler (mindsdb/api/mcp/completions.py).
"""

import asyncio
from unittest.mock import MagicMock, patch

from mcp.types import PromptReference, ResourceTemplateReference
from mcp.shared.memory import create_connected_server_and_client_session

from mindsdb.api.mcp.mcp_instance import mcp

# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_PATCH_GET_DB_NAMES = "mindsdb.api.mcp.completions._get_database_names"
_PATCH_CTX = "mindsdb.api.mcp.completions.ctx"
_PATCH_SESSION = "mindsdb.api.mcp.completions.SessionController"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


def _complete(ref, argument: dict, context_arguments: dict | None = None) -> list[str]:
    """Run a completion request and return the list of completion values."""

    async def _inner():
        async with create_connected_server_and_client_session(mcp) as client:
            result = await client.complete(
                ref=ref,
                argument=argument,
                context_arguments=context_arguments,
            )
            return result.completion.values

    return _run(_inner())


_PROMPT_REF = PromptReference(type="ref/prompt", name="sample_table")
_RESOURCE_REF = ResourceTemplateReference(
    type="ref/resource",
    uri="schema://databases/{database_name}/tables",
)


def _make_table_mock(name: str) -> MagicMock:
    t = MagicMock()
    t.TABLE_NAME = name
    return t


class TestDatabaseNameCompletion:
    def test_returns_matching_databases(self):
        with patch(_PATCH_GET_DB_NAMES, return_value=["pg_prod", "pg_staging", "mysql_db"]):
            values = _complete(_PROMPT_REF, {"name": "database_name", "value": "pg"})

        assert values == ["pg_prod", "pg_staging"]

    def test_prefix_filters_case_sensitively(self):
        with patch(_PATCH_GET_DB_NAMES, return_value=["Postgres", "postgres"]):
            values = _complete(_PROMPT_REF, {"name": "database_name", "value": "post"})

        assert values == ["postgres"]

    def test_empty_prefix_returns_all_databases(self):
        db_names = ["pg", "mysql", "mongo"]
        with patch(_PATCH_GET_DB_NAMES, return_value=db_names):
            values = _complete(_PROMPT_REF, {"name": "database_name", "value": ""})

        assert values == db_names

    def test_no_match_returns_empty_list(self):
        with patch(_PATCH_GET_DB_NAMES, return_value=["pg", "mysql"]):
            values = _complete(_PROMPT_REF, {"name": "database_name", "value": "oracle"})

        assert values == []


class TestTableNameCompletion:
    def test_returns_matching_tables(self):
        with patch(_PATCH_SESSION) as SC:
            SC.return_value.datahub.get.return_value.get_tables.return_value = [
                _make_table_mock("orders"),
                _make_table_mock("order_items"),
                _make_table_mock("users"),
            ]

            # match 2/3
            values = _complete(
                _RESOURCE_REF,
                {"name": "table_name", "value": "ord"},
                context_arguments={"database_name": "pg"},
            )

            SC.return_value.datahub.get.assert_called_with("pg")
            assert values == ["orders", "order_items"]

            # match all
            values = _complete(
                _RESOURCE_REF,
                {"name": "table_name", "value": ""},
                context_arguments={"database_name": "pg"},
            )

            assert values == ["orders", "order_items", "users"]

            # match 0
            values = _complete(
                _RESOURCE_REF,
                {"name": "table_name", "value": "qwerty"},
                context_arguments={"database_name": "pg"},
            )

            assert values == []

    def test_missing_database_name_context_returns_empty(self):
        """When database_name is not in context_arguments, return empty."""
        with patch(_PATCH_SESSION):
            values = _complete(
                _RESOURCE_REF,
                {"name": "table_name", "value": "ord"},
                context_arguments=None,
            )

        assert values == []

    def test_unknown_argument_name_returns_empty(self):
        values = _complete(_PROMPT_REF, {"name": "unknown_param", "value": "foo"})
        assert values == []
