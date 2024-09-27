import pytest
from mindsdb.integrations.handlers.openai_handler.reranker import Reranker

DOCUMENT_EXAMPLE = """The former home secretary Suella Braverman acted unlawfully in making it easier for the police to criminalise peaceful protests, the high court has ruled. She was found to have both acted outside her powers and to have failed to consult properly over regulations that would be likely to increase prosecutions of protesters by a third. Hundreds of protesters have been arrested since the government redefined the sort of protest that could be restricted by the police, allowing it where there is merely a “more than minor” hindrance to people’s daily lives."""
QUERY_EXAMPLE = "What happened with Suella Braverman?"
NEGATIVE_QUERY_EXAMPLE = "What happened with Rishi Sunak?"


async def invoke_reranker(reranker, query, document_list):
    inputs = {"query": query, "documents": document_list}
    return await reranker.invoke(inputs)


@pytest.mark.asyncio
async def test_reranker():
    reranker = Reranker()
    output = await invoke_reranker(
        reranker, QUERY_EXAMPLE, [DOCUMENT_EXAMPLE, NEGATIVE_QUERY_EXAMPLE]
    )
    assert output is not None, "Reranker should return a non-null output"
    assert isinstance(output, list), "Reranker output should be a list"
    assert (output[0][1] > 0.8), "Reranker should have assigned a high score to the correct answer"
    # assert all results have high score (results with low score were filtered out
    assert all([score > 0.8 for doc, score in output])


@pytest.mark.asyncio
async def test_reranker_batch():
    reranker = Reranker()
    output = await invoke_reranker(
        reranker, QUERY_EXAMPLE, [DOCUMENT_EXAMPLE, NEGATIVE_QUERY_EXAMPLE] * 10
    )
    assert output is not None, "Reranker should return a non-null output"
    assert isinstance(output, list), "Reranker output should be a list"
    assert (output[0][1] > 0.8), "Reranker should have assigned a high score to the correct answer"
    assert all([score > 0.8 for doc, score in output])
