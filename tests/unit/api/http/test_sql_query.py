"""
Tests for POST /sql/query endpoint with different response_format values:
1. DEFAULT (None) - returns JSON response
2. SSE ("sse") - returns Server-Sent Events stream
3. JSONLINES ("jsonlines") - returns JSON Lines stream
"""

import json
from http import HTTPStatus
from unittest.mock import patch, MagicMock

import pandas as pd

from mindsdb.api.executor.data_types.sql_answer import SQLAnswer
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.utilities.types.column import Column


def create_mock_sql_answer():
    """Create a mock SQLAnswer with table data for testing."""
    columns = [
        Column(name="id", alias="id"),
        Column(name="name", alias="name"),
        Column(name="value", alias="value"),
    ]

    df = pd.DataFrame([
        [1, "test1", 100],
        [2, "test2", 200],
        [3, "test3", 300],
    ])

    result_set = ResultSet(columns=columns, df=df)

    return SQLAnswer(
        resp_type=RESPONSE_TYPE.TABLE,
        result_set=result_set,
    )


def check_response(response_data: dict):
    # Check response structure for default format
    assert response_data["type"] == "table"
    assert "data" in response_data
    assert "column_names" in response_data
    assert "context" in response_data

    # Check data content
    assert response_data["column_names"] == ["id", "name", "value"]
    assert len(response_data["data"]) == 3
    assert response_data["data"][0] == [1, "test1", 100]
    assert response_data["data"][1] == [2, "test2", 200]
    assert response_data["data"][2] == [3, "test3", 300]


def setup_mock_proxy(mock_proxy_class):
    """Configure mock proxy with default behavior."""
    mock_proxy = MagicMock()
    mock_proxy_class.return_value = mock_proxy
    mock_proxy.process_query.return_value = create_mock_sql_answer()
    mock_proxy.get_context.return_value = {}
    return mock_proxy


class TestSQLQueryResponseFormat:
    @patch("mindsdb.api.http.namespaces.sql.FakeMysqlProxy")
    def test_query_default_format(self, mock_proxy_class, client):
        """Test POST /sql/query with default response format (no response_format parameter).
        """
        setup_mock_proxy(mock_proxy_class)

        response = client.post(
            "/api/sql/query",
            json={"query": "SELECT * FROM table"},
        )

        assert response.status_code == HTTPStatus.OK
        response_data = response.json
        check_response(response_data)

    @patch("mindsdb.api.http.namespaces.sql.FakeMysqlProxy")
    def test_query_sse_format(self, mock_proxy_class, client):
        """Test POST /sql/query with SSE response format (response_format="sse").
        """
        setup_mock_proxy(mock_proxy_class)

        response = client.post(
            "/api/sql/query",
            json={
                "query": "SELECT * FROM table",
                "response_format": "sse",
            },
        )

        assert response.status_code == HTTPStatus.OK
        assert "text/event-stream" in response.content_type

        # Parse SSE response and build unified response dict
        response_text = response.get_data(as_text=True)
        lines = [line.replace("data: ", "") for line in response_text.split("\n") if line.startswith("data: ")]

        assert len(lines) > 1
        header = json.loads(lines[0])
        data_rows = json.loads(lines[1])

        response_data = {
            "type": header["type"],
            "column_names": header["column_names"],
            "data": data_rows,
            "context": {},
        }
        check_response(response_data)

    @patch("mindsdb.api.http.namespaces.sql.FakeMysqlProxy")
    def test_query_jsonlines_format(self, mock_proxy_class, client):
        """Test POST /sql/query with JSONLINES response format (response_format="jsonlines").
        """
        setup_mock_proxy(mock_proxy_class)

        response = client.post(
            "/api/sql/query",
            json={
                "query": "SELECT * FROM table",
                "response_format": "jsonlines",
            },
        )

        assert response.status_code == HTTPStatus.OK
        assert response.content_type == "application/jsonlines"

        # Parse JSONLINES response and build unified response dict
        response_text = response.get_data(as_text=True)
        lines = [line for line in response_text.split("\n") if line.strip()]

        assert len(lines) > 1
        header = json.loads(lines[0])
        data_rows = json.loads(lines[1])

        response_data = {
            "type": header["type"],
            "column_names": header["column_names"],
            "data": data_rows,
            "context": {},
        }
        check_response(response_data)
