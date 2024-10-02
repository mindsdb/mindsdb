from typing import List, Tuple, Dict, Any

from langchain_core.runnables import RunnableParallel

from openai import AsyncOpenAI
import asyncio
import math
import os


class Reranker(RunnableParallel):
    """
    Reranker class for reranking query-document pairs based on relevance
    using OpenAI's model.
    """
    filtering: bool = False
    filtering_threshold: float = 0.5

    # Assume the OpenAI API Key is set

    async def _call_reranker(self, query_document_pairs: List[Tuple[str, str]]) -> List[Tuple[str, float]]:
        """
        Calls the relevancy ranking logic on the given query-document pairs.
        """

        results = await asyncio.gather(
            *[self.search_relevancy(query=query, document=document) for (query, document) in query_document_pairs]
        )
        ranked_results = []
        for idx, result in enumerate(results):
            prob = math.exp(result.choices[0].logprobs.content[0].logprob)
            answer = result.choices[0].logprobs.content[0].token
            if answer == "YES":
                score = prob
            elif answer == "NO":
                score = 1 - prob
            else:
                score = 0.0

            ranked_results.append((query_document_pairs[idx][1], score))  # (document, score)

        # Sort documents by relevance score
        ranked_results.sort(key=lambda x: x[1], reverse=True)
        return ranked_results

    async def search_relevancy(self, query: str, document: str) -> Any:
        openai_api_key = os.environ.get("OPENAI_API_KEY")

        client = AsyncOpenAI(api_key=openai_api_key)

        message_history = [
            {
                "role": "system",
                "content": """Your task is to classify whether the document is relevant to the search query provided below. Answer just "YES" or "NO". """
            },
            {
                "role": "user",
                "content": f"""Document: ```{document}```; Search query: ```{query}```"""
            },
        ]
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=message_history,
            temperature=0,
            logprobs=True,
            max_tokens=1,
        )
        return response

    async def invoke(self, inputs: Dict[str, Any]) -> List[Tuple[str, float]]:
        """
        Main method invoked by LangChain.
        Inputs are expected to be a dictionary with 'query' and 'documents'.
        """
        query = inputs["query"]
        documents = inputs["documents"]

        # Pair the query with each document
        query_document_pairs = [(query, doc) for doc in documents]

        # Call the reranker to rank the documents
        reranked_documents = await self._call_reranker(query_document_pairs)

        # filter out reranked documents with the score lower than 0.5
        reranked_documents = [(document, score) for document, score in reranked_documents if score >= 0.5]

        # Return reranked documents
        return reranked_documents
