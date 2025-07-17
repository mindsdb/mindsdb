from langchain.schema import Document
import pytest

from mindsdb.integrations.utilities.rag.rerankers.reranker_compressor import LLMReranker
from mindsdb.integrations.utilities.rag.settings import RerankerConfig


@pytest.mark.asyncio
async def test_openai_reranker():
    openai_reranker = LLMReranker()
    results = await openai_reranker.compress_documents(
        documents=[Document(page_content="Jack declared that he likes cats more than dogs"),
                   Document(page_content="Jack declared that he likes AI")],
        query="Jack's opinion on animals",
    )
    assert len(results) == 1
    assert "cats" in results[0].page_content


@pytest.mark.asyncio
async def test_openai_reranker_diff_threshold():
    openai_reranker = LLMReranker(filtering_threshold=0.6)
    assert openai_reranker.filtering_threshold == 0.6
    results = await openai_reranker.compress_documents(
        documents=[Document(page_content="Jack declared that he likes cats more than dogs"),
                   Document(page_content="Jack declared that he likes AI")],
        query="Jack's opinion on animals",
    )
    assert len(results) == 1
    assert "cats" in results[0].page_content
    assert openai_reranker.filtering_threshold == 0.6


@pytest.mark.asyncio
async def test_openai_reranker_config():
    config = RerankerConfig(filtering_threshold=0.6, model="gpt-3.5-turbo", base_url="https://api.openai.com/v1")
    openai_reranker = LLMReranker(filtering_threshold=config.filtering_threshold, model=config.model,
                                  base_url=config.base_url)
    assert openai_reranker.filtering_threshold == 0.6
    results = await openai_reranker.compress_documents(
        documents=[Document(page_content="Jack declared that he likes cats more than dogs"),
                   Document(page_content="Jack declared that he likes AI")],
        query="Jack's opinion on animals",
    )
    assert len(results) == 1
    assert "cats" in results[0].page_content
    assert openai_reranker.filtering_threshold == 0.6
