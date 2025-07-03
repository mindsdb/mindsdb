from typing import List, Any, Optional

import pytest
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage

from mindsdb.integrations.utilities.rag.retrievers import MultiHopRetriever


# Simple template for testing
TEST_TEMPLATE = """Question: {question}
Context: {context}
Generate follow-up questions:"""


class MockRetriever(BaseRetriever):
    """Simple mock retriever that returns predefined documents"""
    def _get_relevant_documents(self, query: str, **kwargs) -> List[Document]:
        if "Wright brothers" in query:
            return [Document(page_content="The Wright brothers invented the airplane.")]
        if "World War 1" in query:
            return [Document(page_content="Airplanes were used extensively in WWI.")]
        return []


class MockLLM(BaseChatModel):
    """Simple mock LLM that returns predefined responses"""
    @property
    def _llm_type(self) -> str:
        return "mock"

    def _generate(self, messages: List[BaseMessage], stop: Optional[List[str]] = None, run_manager: Optional[Any] = None, **kwargs) -> Any:
        raise NotImplementedError("Not needed for tests")

    def invoke(self, input_str: str, **kwargs) -> str:
        if "Wright brothers" in str(input_str):
            return '["How were airplanes used in World War 1?"]'
        return "[]"


class InvalidOutputLLM(BaseChatModel):
    """Mock LLM that always returns invalid JSON"""
    @property
    def _llm_type(self) -> str:
        return "mock"

    def _generate(self, messages: List[BaseMessage], stop: Optional[List[str]] = None, run_manager: Optional[Any] = None, **kwargs) -> Any:
        raise NotImplementedError("Not needed for tests")

    def invoke(self, input_str: str, **kwargs) -> str:
        return "invalid json"


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
        max_hops=2,
        reformulation_template=TEST_TEMPLATE
    )

    # Test with a query that should trigger follow-up
    docs = retriever._get_relevant_documents("Tell me about the Wright brothers")

    # Should have documents from both queries
    assert len(docs) == 2
    assert any("Wright brothers" in doc.page_content for doc in docs)
    assert any("WWI" in doc.page_content for doc in docs)


def test_multi_hop_retriever_no_results(mock_retriever, mock_llm):
    """Test behavior when no documents are found"""
    retriever = MultiHopRetriever(
        base_retriever=mock_retriever,
        llm=mock_llm,
        max_hops=2,
        reformulation_template=TEST_TEMPLATE
    )

    # Test with a query that won't find any documents
    docs = retriever._get_relevant_documents("Something unrelated")

    # Should have no documents
    assert len(docs) == 0


def test_multi_hop_retriever_invalid_llm_output(mock_retriever):
    """Test handling of invalid LLM output"""
    retriever = MultiHopRetriever(
        base_retriever=mock_retriever,
        llm=InvalidOutputLLM(),
        max_hops=2,
        reformulation_template=TEST_TEMPLATE
    )

    # Should still work and return initial results
    docs = retriever._get_relevant_documents("Tell me about the Wright brothers")
    assert len(docs) == 1
    assert "Wright brothers" in docs[0].page_content


def test_multi_hop_retriever_max_hops(mock_retriever, mock_llm):
    """Test that max_hops is respected"""
    retriever = MultiHopRetriever(
        base_retriever=mock_retriever,
        llm=mock_llm,
        max_hops=1,  # Only allow 1 hop
        reformulation_template=TEST_TEMPLATE
    )

    # Should only get initial documents
    docs = retriever._get_relevant_documents("Tell me about the Wright brothers")
    assert len(docs) == 1
    assert "Wright brothers" in docs[0].page_content
