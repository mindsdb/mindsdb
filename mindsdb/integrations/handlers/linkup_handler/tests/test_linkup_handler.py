import pytest
from linkup_handler.linkup_handler import LinkupSearchTool

# Mocking the LinkupClient for testing
class MockLinkupClient:
    def search(self, query, depth, output_type):
        if query == "valid_query":
            return MockResponse(
                results=[
                    MockResult(name="Result 1", url="https://example.com/1", content="Content 1"),
                    MockResult(name="Result 2", url="https://example.com/2", content="Content 2"),
                ]
            )
        elif query == "error_query":
            raise Exception("Mock API error")
        else:
            return MockResponse(results=[])

class MockResponse:
    def __init__(self, results):
        self.results = results

class MockResult:
    def __init__(self, name, url, content):
        self.name = name
        self.url = url
        self.content = content

@pytest.fixture
def mock_linkup_tool(monkeypatch):
    tool = LinkupSearchTool(api_key="mock_api_key")
    monkeypatch.setattr(tool, "_client", MockLinkupClient())
    return tool

def test_run_search_success(mock_linkup_tool):
    response = mock_linkup_tool._run(query="valid_query", depth="standard", output_type="searchResults")
    assert response["success"] is True
    assert len(response["results"]) == 2
    assert response["results"][0]["name"] == "Result 1"
    assert response["results"][0]["url"] == "https://example.com/1"
    assert response["results"][0]["content"] == "Content 1"

def test_run_search_no_results(mock_linkup_tool):
    response = mock_linkup_tool._run(query="no_results_query", depth="standard", output_type="searchResults")
    assert response["success"] is True
    assert len(response["results"]) == 0

def test_run_search_error(mock_linkup_tool):
    response = mock_linkup_tool._run(query="error_query", depth="standard", output_type="searchResults")
    assert response["success"] is False
    assert "error" in response
    assert response["error"] == "Mock API error"
