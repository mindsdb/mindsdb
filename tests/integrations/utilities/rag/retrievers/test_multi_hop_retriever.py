from typing import List
from unittest.mock import MagicMock

import pytest
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever

from mindsdb.integrations.utilities.rag.retrievers import MultiHopRetriever


class MockRetriever(BaseRetriever):
    """Simple mock retriever that returns predefined documents"""
    def _get_relevant_documents(self, query: str, **kwargs) -> List[Document]:
        if "Wright brothers" in query:
            return [Document(page_content="The Wright brothers invented the airplane.")]
        return []


class MockLLM:
    """Simple mock LLM that returns predefined responses"""
    def invoke(self, input_str: str, **kwargs) -> str:
        if "Wright brothers" in str(input_str):
            return '["How were airplanes used in World War 1 military operations?"]'
        return "[]"


@pytest.fixture
def mock_retriever():
    return MockRetriever()


@pytest.fixture
def mock_llm():
    return MockLLM()


def test_multi_hop_retriever_basic_functionality(mock_retriever, mock_llm):
    """Test the basic functionality of MultiHopRetriever"""
    retriever = MultiHopRetriever(
        base_retriever=mock_retriever,
        llm=mock_llm,
        max_hops=2
    )

    # Test with a query that should trigger follow-up
    docs = retriever._get_relevant_documents("Tell me about the Wright brothers")

    # Should have documents from initial query
    assert len(docs) > 0
    assert "Wright brothers" in docs[0].page_content


def test_multi_hop_retriever_no_results(mock_retriever, mock_llm):
    """Test behavior when no documents are found"""
    retriever = MultiHopRetriever(
        base_retriever=mock_retriever,
        llm=mock_llm,
        max_hops=2
    )

    # Test with a query that won't find any documents
    docs = retriever._get_relevant_documents("Something unrelated")

    # Should have no documents
    assert len(docs) == 0


def test_multi_hop_retriever_invalid_llm_output(mock_retriever, mock_llm):
    """Test handling of invalid LLM output"""
    # Create mock LLM that returns invalid JSON
    invalid_llm = MockLLM()
    invalid_llm.invoke = MagicMock(return_value="invalid json")

    retriever = MultiHopRetriever(
        base_retriever=mock_retriever,
        llm=invalid_llm,
        max_hops=2
    )

    # Should still work and return initial results
    docs = retriever._get_relevant_documents("Tell me about the Wright brothers")
    assert len(docs) == 1
    assert "Wright brothers" in docs[0].page_content
