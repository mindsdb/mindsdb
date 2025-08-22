from mindsdb.integrations.utilities.sql_utils import KeywordSearchArgs, FilterCondition
import pytest
from unittest.mock import patch, MagicMock
from mindsdb_sql_parser import parse_sql
import pandas as pd

# Assume the code you provided is in a file named 'kb_table.py'
from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseTable
from mindsdb.integrations.libs.keyword_search_base import KeywordSearchBase


class MockVectorStoreHandler(KeywordSearchBase):
    """A mock VectorStoreHandler that returns predefined data for testing."""

    def __init__(self, vector_search_data: pd.DataFrame, keyword_search_data: pd.DataFrame = None):
        self._vector_data = vector_search_data
        self._keyword_data = keyword_search_data if keyword_search_data is not None else pd.DataFrame()

    def dispatch_select(
        self,
        query,
        conditions: list[FilterCondition],
        keyword_search_args: KeywordSearchArgs = None,
        allowed_metadata_columns=None,
    ) -> pd.DataFrame:
        # Return the predefined keyword search data.
        if keyword_search_args:
            return self._vector_data.copy()
        print("MockVectorStoreHandler.dispatch_keyword_select called!")
        return self._keyword_data.copy()

    def extract_conditions(self, where_clause):
        if where_clause is None:
            return []
        return [
            FilterCondition(column="content", op="=", value="test query"),
            FilterCondition(column="hybrid_search", op="=", value=True),
        ]


@pytest.fixture
def mock_kb_table():
    """Pytest fixture to set up a KnowledgeBaseTable with mock dependencies."""

    # 1. Define the fake data our mock DB will return
    mock_vector_results = pd.DataFrame(
        {
            "id": ["doc1_chunk1", "doc2_chunk1"],
            "content": ["some vector content", "more vector content"],
            "metadata": [{"original_doc_id": "doc1"}, {"original_doc_id": "doc2"}],
            "distance": [0.1, 0.5],
        }
    )

    mock_keyword_results = pd.DataFrame(
        {
            "id": ["doc3_chunk1"],
            "content": ["keyword match content"],
            "metadata": [{"original_doc_id": "doc3"}],
            "distance": [0.1],
        }
    )

    # 2. Create the mock handler with the fake data
    mock_db_handler = MockVectorStoreHandler(
        vector_search_data=mock_vector_results, keyword_search_data=mock_keyword_results
    )

    # 3. Mock the KnowledgeBase object and session
    mock_kb = MagicMock()
    mock_kb.params = {}
    mock_kb.vector_database_table = "mock_table"

    mock_session = MagicMock()

    # 4. Create the instance of the class we are testing
    kb_table = KnowledgeBaseTable(kb=mock_kb, session=mock_session)

    # 5. Inject our mock database handler
    kb_table._vector_db = mock_db_handler

    return kb_table


def test_hybrid_search_merges_results(mock_kb_table):
    """
    Tests that select_query correctly merges vector and keyword search results
    when hybrid_search is enabled.
    """
    # ARRANGE
    # Use patch to prevent the test from trying to create a real embedding
    with patch.object(mock_kb_table, "_content_to_embeddings", return_value=[0.1, 0.2, 0.3]) as mock_embedding:
        # A simple query that will trigger the logic we want to test
        query_str = "SELECT * FROM my_kb WHERE content = 'test query' AND hybrid_search = TRUE"
        query = parse_sql(query_str, dialect="mindsdb")

        # ACT
        result_df = mock_kb_table.select_query(query)

        # ASSERT
        # We expect 3 rows: 2 from vector search + 1 from keyword search
        assert len(result_df) == 3

        # Check that the embedding function was called
        mock_embedding.assert_called_once_with("test query")

        # Check that the results are sorted by relevance (descending)
        # Relevance is 1 / (1 + distance)
        assert result_df["relevance"].is_monotonic_decreasing

        # Check that the keyword result is present
        assert "doc3" in result_df["id"].values
