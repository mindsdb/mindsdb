"""
Unit tests for the MCP tools (mindsdb/api/mcp/tools/*).
"""

import asyncio
import json
from unittest.mock import patch


_PATCH_PROXY = "mindsdb.api.mcp.tools.query.FakeMysqlProxy"


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


def _call_tool(sql: str, context=None):
    """Call the MCP query tool synchronously and return parsed JSON."""
    args = {"query": sql}
    if context is not None:
        args["context"] = context

    from mindsdb.api.mcp.mcp_instance import mcp

    content, _ = _run(mcp.call_tool("query", args))
    return json.loads(content[0].text)


def _make_proxy_ok(mock_proxy_cls, affected_rows=0):
    """Configure mock proxy to return an OK response."""
    mock_proxy_cls.return_value.process_query.return_value.dump_http_response.return_value = {
        "type": "ok",
        "affected_rows": affected_rows,
    }
    return mock_proxy_cls.return_value


def _make_proxy_table(mock_proxy_cls, column_names, data):
    """Configure mock proxy to return a table response."""
    mock_proxy_cls.return_value.process_query.return_value.dump_http_response.return_value = {
        "type": "table",
        "column_names": column_names,
        "data": data,
    }
    return mock_proxy_cls.return_value


def _make_proxy_error(mock_proxy_cls, error_message, error_code=0):
    """Configure mock proxy to return an error response."""
    mock_proxy_cls.return_value.process_query.return_value.dump_http_response.return_value = {
        "type": "error",
        "error_code": error_code,
        "error_message": error_message,
    }
    return mock_proxy_cls.return_value


class TestResponseTypes:
    def test_select_returns_table_type(self):
        expected_data = [[1, "alice"], [2, "bob"]]
        columns_list = ["id", "name"]
        with patch(_PATCH_PROXY) as MockProxy:
            _make_proxy_table(MockProxy, columns_list, expected_data)
            result = _call_tool("SELECT * FROM mydb.users")

        assert result["type"] == "table"
        assert result["column_names"] == columns_list
        assert result["data"] == expected_data

    def test_select_empty_result(self):
        columns_list = ["id", "name"]
        with patch(_PATCH_PROXY) as MockProxy:
            _make_proxy_table(MockProxy, columns_list, [])
            result = _call_tool("SELECT * FROM mydb.users WHERE 1=0")

        assert result["type"] == "table"
        assert result["column_names"] == columns_list
        assert result["data"] == []

    def test_insert_returns_ok_type(self):
        with patch(_PATCH_PROXY) as MockProxy:
            _make_proxy_ok(MockProxy, affected_rows=1)
            result = _call_tool("INSERT INTO mydb.t (id) VALUES (1)")

        assert result["type"] == "ok"
        assert result["affected_rows"] == 1

    def test_proxy_error_response_returns_error_type(self):
        error_message = "Table 'x' doesn't exist"
        with patch(_PATCH_PROXY) as MockProxy:
            _make_proxy_error(MockProxy, error_message, error_code=123)
            result = _call_tool("SELECT * FROM mydb.x")

        assert result["type"] == "error"
        assert result["error_message"] == error_message
        assert result["error_code"] == 123

    def test_exception_in_process_query_returns_error_type(self):
        error_message = "connection refused"
        with patch(_PATCH_PROXY) as MockProxy:
            MockProxy.return_value.process_query.side_effect = Exception(error_message)
            result = _call_tool("SELECT 1")

        assert result["type"] == "error"
        assert result["error_message"] == error_message


class TestContextParameter:
    def test_context_is_passed_to_set_context(self):
        with patch(_PATCH_PROXY) as MockProxy:
            proxy = _make_proxy_ok(MockProxy)
            _call_tool("SELECT 1", context={"db": "my_postgres"})

        proxy.set_context.assert_called_once_with({"db": "my_postgres"})

    def test_omitted_context_defaults_to_empty_dict(self):
        with patch(_PATCH_PROXY) as MockProxy:
            proxy = _make_proxy_ok(MockProxy)
            _call_tool("SELECT 1")  # no context argument

        proxy.set_context.assert_called_once_with({})

    def test_explicit_none_context_defaults_to_empty_dict(self):
        with patch(_PATCH_PROXY) as MockProxy:
            proxy = _make_proxy_ok(MockProxy)
            _call_tool("SELECT 1", context=None)

        proxy.set_context.assert_called_once_with({})
