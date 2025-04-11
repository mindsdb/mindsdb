from __future__ import annotations

import asyncio
import logging
import math
import os
import random
from typing import Any, Dict, List, Optional, Sequence, Tuple

from langchain.retrievers.document_compressors.base import BaseDocumentCompressor
from langchain_core.callbacks import Callbacks, dispatch_custom_event
from langchain_core.documents import Document
from openai import AsyncOpenAI

from mindsdb.integrations.utilities.rag.settings import DEFAULT_RERANKING_MODEL, DEFAULT_LLM_ENDPOINT

log = logging.getLogger(__name__)


class LLMReranker(BaseDocumentCompressor):
    filtering_threshold: float = 0.0  # Default threshold for filtering
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

    async def search_relevancy(self, query: str, document: str, custom_event: bool = True) -> Any:
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
                    rerank_data = {
                        "document": document,
                        "answer": answer,
                        "logprob": logprob
                    }

                    # Stream reranking update.
                    if custom_event:
                        dispatch_custom_event("rerank", rerank_data)
                    return rerank_data

                except Exception as e:
                    if attempt == self.max_retries - 1:
                        log.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                        raise
                    # Exponential backoff with jitter
                    retry_delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    await asyncio.sleep(retry_delay)

    async def _rank(self, query_document_pairs: List[Tuple[str, str]], custom_event: bool = True) -> List[Tuple[str, float]]:
        ranked_results = []

        # Process in larger batches for better throughput
        batch_size = min(self.max_concurrent_requests * 2, len(query_document_pairs))
        for i in range(0, len(query_document_pairs), batch_size):
            batch = query_document_pairs[i:i + batch_size]
            try:
                results = await asyncio.gather(
                    *[self.search_relevancy(query=query, document=document, custom_event=custom_event) for (query, document) in batch],
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
                    try:
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
                        # Don't let early stopping errors stop the whole process
                        log.warning(f"Error in early stopping check: {str(e)}")

            except Exception as e:
                log.error(f"Batch processing error: {str(e)}")
                continue
        return ranked_results

    async def search_relevancy_score(self, query: str, document: str) -> Any:
        await self._init_client()

        async with self._semaphore:
            for attempt in range(self.max_retries):
                try:
                    response = await self.client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {"role": "system", "content": """
                                You are an intelligent assistant that evaluates how relevant a given document chunk is to a user's search query.
                                Your task is to analyze the similarity between the search query and the document chunk, and return **only the bucket label** that best represents the relevance:

                                - "bucket_1": Not relevant (score between 0.0 and 0.25)
                                - "bucket_2": Slightly relevant (score between 0.25 and 0.5)
                                - "bucket_3": Moderately relevant (score between 0.5 and 0.75)
                                - "bucket_4": Highly relevant (score between 0.75 and 1.0)

                                Respond with only one of: "bucket_1", "bucket_2", "bucket_3", or "bucket_4".

                                Examples:

                                Search query: "How to reset a router to factory settings?"
                                Document chunk: "Computers often come with customizable parental control settings."
                                Score: bucket_1

                                Search query: "Symptoms of vitamin D deficiency"
                                Document chunk: "Vitamin D deficiency has been linked to fatigue, bone pain, and muscle weakness."
                                Score: bucket_4

                                Search query: "Best practices for onboarding remote employees"
                                Document chunk: "An employee handbook can be useful for new hires, outlining company policies and benefits."
                                Score: bucket_2

                                Search query: "Benefits of mindfulness meditation"
                                Document chunk: "Practicing mindfulness has shown to reduce stress and improve focus in multiple studies."
                                Score: bucket_3

                                Search query: "What is Kubernetes used for?"
                                Document chunk: "Kubernetes is an open-source system for automating deployment, scaling, and management of containerized applications."
                                Score: bucket_4

                                Search query: "How to bake sourdough bread at home"
                                Document chunk: "The French Revolution began in 1789 and radically transformed society."
                                Score: bucket_1

                                Search query: "Machine learning algorithms for image classification"
                                Document chunk: "Convolutional Neural Networks (CNNs) are particularly effective in image classification tasks."
                                Score: bucket_4

                                Search query: "How to improve focus while working remotely"
                                Document chunk: "Creating a dedicated workspace and setting a consistent schedule can significantly improve focus during remote work."
                                Score: bucket_4

                                Search query: "Carbon emissions from electric vehicles vs gas cars"
                                Document chunk: "Electric vehicles produce zero emissions while driving, but battery production has environmental impacts."
                                Score: bucket_3

                                Search query: "Time zones in the United States"
                                Document chunk: "The U.S. is divided into six primary time zones: Eastern, Central, Mountain, Pacific, Alaska, and Hawaii-Aleutian."
                                Score: bucket_4
                             """},

                            {"role": "user", "content": f"""
                                Now evaluate the following pair:

                                Search query: {query}
                                Document chunk: {document}

                                Which bucket best represents the relevance?
                            """}
                        ],
                        temperature=self.temperature,
                        n=1,
                        logprobs=True,
                        top_logprobs=4,
                        max_tokens=3
                    )

                    # Extract response and logprobs
                    bucket = response.choices[0].message.content.strip()
                    token_logprobs = response.choices[0].logprobs.content
                    # Reconstruct the prediction and extract the top logprobs from the final token (e.g., "1")
                    final_token_logprob = token_logprobs[-1]
                    top_logprobs = final_token_logprob.top_logprobs
                    # Create a map of 'bucket_1' -> probability, using token combinations
                    bucket_probs = {}
                    for top_token in top_logprobs:
                        full_label = f"bucket_{top_token.token}"
                        prob = math.exp(top_token.logprob)
                        bucket_probs[full_label] = prob
                    # Optional: normalize in case some are missing
                    total_prob = sum(bucket_probs.values())
                    bucket_probs = {k: v / total_prob for k, v in bucket_probs.items()}
                    # Assign weights to buckets (midpoints of ranges)
                    bucket_weights = {
                        "bucket_1": 0.25,
                        "bucket_2": 0.5,
                        "bucket_3": 0.75,
                        "bucket_4": 1.0
                    }
                    # Compute the final smooth score
                    relevance_score = sum(bucket_weights.get(bucket, 0) * prob for bucket, prob in bucket_probs.items())
                    rerank_data = {
                        "document": document,
                        "answer": bucket,
                        "relevance_score": relevance_score
                    }
                    return rerank_data

                except Exception as e:
                    if attempt == self.max_retries - 1:
                        log.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                        raise
                    # Exponential backoff with jitter
                    retry_delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    await asyncio.sleep(retry_delay)

    async def _rank_score(self, query_document_pairs: List[Tuple[str, str]]) -> List[Tuple[str, float]]:
        ranked_results = []

        # Process in larger batches for better throughput
        batch_size = min(self.max_concurrent_requests * 2, len(query_document_pairs))
        for i in range(0, len(query_document_pairs), batch_size):
            batch = query_document_pairs[i:i + batch_size]
            try:
                results = await asyncio.gather(
                    *[self.search_relevancy_score(query=query, document=document) for (query, document) in batch],
                    return_exceptions=True
                )

                for idx, result in enumerate(results):
                    if isinstance(result, Exception):
                        log.error(f"Error processing document {i+idx}: {str(result)}")
                        ranked_results.append((batch[idx][1], 0.0))
                        continue

                    score = result["relevance_score"]
                    if score is not None:
                        if score > 1.0:
                            score = 1.0
                        elif score < 0.0:
                            score = 0.0

                    ranked_results.append((batch[idx][1], score))
                    # Check if we should stop early
                    try:
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
                        # Don't let early stopping errors stop the whole process
                        log.warning(f"Error in early stopping check: {str(e)}")

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

        # Stream reranking update.
        dispatch_custom_event('rerank_begin', {'num_documents': len(documents)})

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

    def get_scores(self, query: str, documents: list[str], custom_event: bool = False):
        query_document_pairs = [(query, doc) for doc in documents]
        # Create event loop and run async code
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # If no running loop exists, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        documents_and_scores = loop.run_until_complete(self._rank_score(query_document_pairs))
        scores = [score for _, score in documents_and_scores]
        return scores
