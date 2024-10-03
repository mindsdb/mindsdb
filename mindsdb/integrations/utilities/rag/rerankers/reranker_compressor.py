from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence, Tuple
from pydantic import BaseModel
from langchain.schema import Document
from langchain_core.callbacks import Callbacks
from langchain.retrievers.document_compressors.base import BaseDocumentCompressor
import logging
import openai
import asyncio
import math
from mindsdb.integrations.handlers.rag_handler.settings import DEFAULT_RERANKING_MODEL
from openai import AsyncOpenAI

log = logging.getLogger(__name__)


class Ranking(BaseModel):
    index: int
    relevance_score: float
    is_relevant: bool


class OpenAIReranker(BaseDocumentCompressor):
    _default_model: str = DEFAULT_RERANKING_MODEL

    filtering_threshold: float = 0.5  # Default threshold for filtering
    model: str = DEFAULT_RERANKING_MODEL  # Model to use for reranking
    temperature: float = 0.0  # Temperature for the model
    openai_api_key: Optional[str] = None
    remove_irrelevant: bool = True  # New flag to control removal of irrelevant documents,
    # by default it will remove irrelevant documents
    top_n: int = 5  # Number of documents to return

    _api_key_var: str = "OPENAI_API_KEY"
    client: Optional[Any] = None

    class Config:
        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        """Initialize the OpenAI client after the model is fully initialized."""
        super().__init__()
        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize the OpenAI client if not already initialized."""
        if not self.client:
            api_key = self.openai_api_key or os.getenv(self._api_key_var)
            if not api_key:
                raise ValueError(
                    f"OpenAI API key must be provided either through the 'openai_api_key' parameter or the {self._api_key_var} environment variable."
                )
            openai.api_key = api_key
            self.client = openai

    def _get_client(self) -> Any:
        """Ensure client is initialized and return it."""
        if not self.client:
            self._initialize_client()
        return self.client

    async def search_relevancy(self, query: str, document: str) -> Any:
        openai_api_key = self.openai_api_key or os.getenv(self._api_key_var)

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

    async def _rank(self, query_document_pairs: List[Tuple[str, str]]) -> List[Tuple[str, float]]:
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

        return ranked_results

    async def compress_documents(
            self,
            documents: Sequence[Document],
            query: str,
            callbacks: Optional[Callbacks] = None,
    ) -> Sequence[Document]:
        """Compress documents using OpenAI's rerank capability with individual document assessment."""
        log.info(f"Compressing documents. Initial count: {len(documents)}")
        if len(documents) == 0 or self.top_n < 1:
            log.warning("No documents to compress or top_n < 1. Returning empty list.")
            return []

        doc_contents = [doc.page_content for doc in documents]
        query_documents_pairs = [(query, doc) for doc in doc_contents]
        rankings = await self._rank(query_documents_pairs)

        compressed = []
        for ind, ranking in enumerate(rankings):
            doc = documents[ind]
            document_text, score = ranking
            doc.metadata["relevance_score"] = score
            doc.metadata["is_relevant"] = score > self.filtering_threshold
            # Add the document to the compressed list if it is relevant or if we are not removing irrelevant documents
            if not self.remove_irrelevant:
                compressed.append(doc)
            elif doc.metadata["is_relevant"]:
                compressed.append(doc)

        log.info(f"Compression complete. {len(compressed)} documents returned")
        if not compressed:
            log.warning("No documents found after compression")

        return compressed

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        """Get the identifying parameters."""
        return {
            "model": self.model,
            "top_n": self.top_n,
            "temperature": self.temperature,
            "remove_irrelevant": self.remove_irrelevant,
        }
