from __future__ import annotations

import asyncio
import logging
import math
import os
import random
from typing import Any, Dict, List, Optional, Sequence, Tuple

from langchain.retrievers.document_compressors.base import BaseDocumentCompressor
from langchain_core.callbacks import Callbacks
from langchain_core.documents import Document
from openai import AsyncOpenAI

from mindsdb.integrations.utilities.rag.settings import DEFAULT_RERANKING_MODEL, DEFAULT_LLM_ENDPOINT

log = logging.getLogger(__name__)


class LLMReranker(BaseDocumentCompressor):
    filtering_threshold: float = 0.5  # Default threshold for filtering
    model: str = DEFAULT_RERANKING_MODEL  # Model to use for reranking
    temperature: float = 0.0  # Temperature for the model
    openai_api_key: Optional[str] = None
    remove_irrelevant: bool = True  # New flag to control removal of irrelevant documents
    base_url: str = DEFAULT_LLM_ENDPOINT
    num_docs_to_keep: Optional[int] = None  # How many of the top documents to keep after reranking & compressing.
    _api_key_var: str = "OPENAI_API_KEY"
    client: Optional[AsyncOpenAI] = None
    _semaphore: Optional[asyncio.Semaphore] = None
    max_concurrent_requests: int = 20
    max_retries: int = 3
    retry_delay: float = 1.0
    request_timeout: float = 20.0  # Timeout for API requests
    early_stop: bool = True  # Whether to enable early stopping
    early_stop_threshold: float = 0.8  # Confidence threshold for early stopping

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    async def _init_client(self):
        if self.client is None:
            openai_api_key = self.openai_api_key or os.getenv(self._api_key_var)
            if not openai_api_key:
                raise ValueError(f"OpenAI API key not found in environment variable {self._api_key_var}")
            self.client = AsyncOpenAI(
                api_key=openai_api_key,
                base_url=self.base_url,
                timeout=self.request_timeout,
                max_retries=2  # Client-level retries
            )

    async def search_relevancy(self, query: str, document: str) -> Any:
        await self._init_client()

        async with self._semaphore:
            for attempt in range(self.max_retries):
                try:
                    response = await self.client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {"role": "system", "content": "Rate the relevance of the document to the query. Respond with 'yes' or 'no'."},
                            {"role": "user", "content": f"Query: {query}\nDocument: {document}\nIs this document relevant?"}
                        ],
                        temperature=self.temperature,
                        n=1,
                        logprobs=True,
                        max_tokens=1
                    )

                    # Extract response and logprobs
                    answer = response.choices[0].message.content
                    logprob = response.choices[0].logprobs.content[0].logprob

                    return {"answer": answer, "logprob": logprob}

                except Exception as e:
                    if attempt == self.max_retries - 1:
                        log.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                        raise
                    # Exponential backoff with jitter
                    retry_delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    await asyncio.sleep(retry_delay)

    async def _rank(self, query_document_pairs: List[Tuple[str, str]]) -> List[Tuple[str, float]]:
        ranked_results = []

        # Process in larger batches for better throughput
        batch_size = min(self.max_concurrent_requests * 2, len(query_document_pairs))
        for i in range(0, len(query_document_pairs), batch_size):
            batch = query_document_pairs[i:i + batch_size]
            try:
                results = await asyncio.gather(
                    *[self.search_relevancy(query=query, document=document) for (query, document) in batch],
                    return_exceptions=True
                )

                for idx, result in enumerate(results):
                    if isinstance(result, Exception):
                        log.error(f"Error processing document {i+idx}: {str(result)}")
                        ranked_results.append((batch[idx][1], 0.0))
                        continue

                    answer = result["answer"]
                    logprob = result["logprob"]
                    prob = math.exp(logprob)

                    # Convert answer to score using the model's confidence
                    if answer.lower().strip() == "yes":
                        score = prob  # If yes, use the model's confidence
                    elif answer.lower().strip() == "no":
                        score = 1 - prob  # If no, invert the confidence
                    else:
                        score = 0.5 * prob  # For unclear answers, reduce confidence

                    ranked_results.append((batch[idx][1], score))

                    # Check if we should stop early
                    high_scoring_docs = [r for r in ranked_results if r[1] >= self.filtering_threshold]
                    can_stop_early = (
                        self.early_stop  # Early stopping is enabled
                        and self.num_docs_to_keep  # We have a target number of docs
                        and len(high_scoring_docs) >= self.num_docs_to_keep  # Found enough good docs
                        and score >= self.early_stop_threshold  # Current doc is good enough
                    )

                    if can_stop_early:
                        log.info(f"Early stopping after finding {self.num_docs_to_keep} documents with high confidence")
                        return ranked_results

            except Exception as e:
                log.error(f"Batch processing error: {str(e)}")
                continue

        return ranked_results

    async def acompress_documents(
        self,
        documents: Sequence[Document],
        query: str,
        callbacks: Optional[Callbacks] = None,
    ) -> Sequence[Document]:
        """Async compress documents using reranking with proper error handling."""
        if callbacks:
            await callbacks.on_retriever_start({"query": query}, "Reranking documents")

        log.info(f"Async compressing documents. Initial count: {len(documents)}")
        if not documents:
            if callbacks:
                await callbacks.on_retriever_end({"documents": []})
            return []

        try:
            # Prepare query-document pairs
            query_document_pairs = [(query, doc.page_content) for doc in documents]

            if callbacks:
                await callbacks.on_text("Starting document reranking...")

            # Get ranked results
            ranked_results = await self._rank(query_document_pairs)

            # Sort by score in descending order
            ranked_results.sort(key=lambda x: x[1], reverse=True)

            # Filter based on threshold and num_docs_to_keep
            filtered_docs = []
            for doc, score in ranked_results:
                if score >= self.filtering_threshold:
                    matching_doc = next(d for d in documents if d.page_content == doc)
                    matching_doc.metadata = {**(matching_doc.metadata or {}), "relevance_score": score}
                    filtered_docs.append(matching_doc)

                    if callbacks:
                        await callbacks.on_text(f"Document scored {score:.2f}")

                    if self.num_docs_to_keep and len(filtered_docs) >= self.num_docs_to_keep:
                        break

            log.info(f"Async compression complete. Final count: {len(filtered_docs)}")

            if callbacks:
                await callbacks.on_retriever_end({"documents": filtered_docs})

            return filtered_docs

        except Exception as e:
            error_msg = f"Error during async document compression: {str(e)}"
            log.error(error_msg)
            if callbacks:
                await callbacks.on_retriever_error(error_msg)
            return documents  # Return original documents on error

    def compress_documents(
        self,
        documents: Sequence[Document],
        query: str,
        callbacks: Optional[Callbacks] = None,
    ) -> Sequence[Document]:
        """Sync wrapper for async compression."""
        return asyncio.run(self.acompress_documents(documents, query, callbacks))

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        """Get the identifying parameters."""
        return {
            "model": self.model,
            "temperature": self.temperature,
            "remove_irrelevant": self.remove_irrelevant,
        }
