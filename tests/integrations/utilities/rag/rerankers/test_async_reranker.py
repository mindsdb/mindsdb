from mindsdb.integrations.utilities.rag.rerankers.reranker_async import LLMReranker, Document
import pytest


@pytest.mark.asyncio
async def test_openai_reranker():
    openai_reranker = LLMReranker()
    results = openai_reranker.compress_documents(
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
