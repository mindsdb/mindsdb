from langchain.schema import Document
import pytest

from mindsdb.integrations.utilities.rag.rerankers.reranker_compressor import OpenAIReranker


@pytest.mark.asyncio
async def test_openai_reranker():
    openai_reranker = OpenAIReranker()
    openai_reranker.model_post_init(None)
    results = await openai_reranker.compress_documents(
        documents=[Document(page_content="Jack declared that he likes cats more than dogs"),
                   Document(page_content="Jack declared that he likes AI")],
        query="Jack's opinion on animals",
    )
    assert len(results) == 1
    assert "cats" in results[0].page_content
