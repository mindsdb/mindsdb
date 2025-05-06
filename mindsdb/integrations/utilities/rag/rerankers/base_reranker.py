from __future__ import annotations

import asyncio
import logging
import math
import os
import random
from abc import ABC
from typing import Any, List, Optional, Tuple

from openai import AsyncOpenAI, AsyncAzureOpenAI
from pydantic import field_validator
from pydantic import BaseModel

from mindsdb.integrations.utilities.rag.settings import DEFAULT_RERANKING_MODEL, DEFAULT_LLM_ENDPOINT

log = logging.getLogger(__name__)


class BaseLLMReranker(BaseModel, ABC):

    filtering_threshold: float = 0.0  # Default threshold for filtering
    provider: str = 'openai'
    model: str = DEFAULT_RERANKING_MODEL  # Model to use for reranking
    temperature: float = 0.0  # Temperature for the model
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    api_version: Optional[str] = None
    num_docs_to_keep: Optional[int] = None  # How many of the top documents to keep after reranking & compressing.
    method: str = "multi-class"  # Scoring method: 'multi-class' or 'binary'
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

    @field_validator('provider')
    @classmethod
    def validate_provider(cls, v: str) -> str:
        allowed = {'openai', 'azure_openai'}
        v_lower = v.lower()
        if v_lower not in allowed:
            raise ValueError(f"Unsupported provider: {v}.")
        return v_lower

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    async def _init_client(self):
        if self.client is None:

            if self.provider == "azure_openai":

                azure_api_key = self.api_key or os.getenv("AZURE_OPENAI_API_KEY")
                azure_api_endpoint = self.base_url or os.environ.get("AZURE_OPENAI_ENDPOINT")
                azure_api_version = self.api_version or os.environ.get("AZURE_OPENAI_API_VERSION")
                self.client = AsyncAzureOpenAI(api_key=azure_api_key,
                                               azure_endpoint=azure_api_endpoint,
                                               api_version=azure_api_version,
                                               timeout=self.request_timeout,
                                               max_retries=2)
            elif self.provider == "openai":
                api_key_var: str = "OPENAI_API_KEY"
                openai_api_key = self.api_key or os.getenv(api_key_var)
                if not openai_api_key:
                    raise ValueError(f"OpenAI API key not found in environment variable {api_key_var}")

                base_url = self.base_url or DEFAULT_LLM_ENDPOINT
                self.client = AsyncOpenAI(api_key=openai_api_key, base_url=base_url, timeout=self.request_timeout, max_retries=2)

    async def search_relevancy(self, query: str, document: str, rerank_callback=None) -> Any:
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
                    if rerank_callback is not None:
                        rerank_callback(rerank_data)

                    return rerank_data

                except Exception as e:
                    if attempt == self.max_retries - 1:
                        log.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                        raise
                    # Exponential backoff with jitter
                    retry_delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    await asyncio.sleep(retry_delay)

    async def _rank(self, query_document_pairs: List[Tuple[str, str]], rerank_callback=None) -> List[Tuple[str, float]]:
        ranked_results = []

        # Process in larger batches for better throughput
        batch_size = min(self.max_concurrent_requests * 2, len(query_document_pairs))
        for i in range(0, len(query_document_pairs), batch_size):
            batch = query_document_pairs[i:i + batch_size]
            try:
                results = await asyncio.gather(
                    *[self.search_relevancy(query=query, document=document, rerank_callback=rerank_callback) for (query, document) in batch],
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
                                Your task is to analyze the similarity between the search query and the document chunk, and return **only the class label** that best represents the relevance:

                                - "class_1": Not relevant (score between 0.0 and 0.25)
                                - "class_2": Slightly relevant (score between 0.25 and 0.5)
                                - "class_3": Moderately relevant (score between 0.5 and 0.75)
                                - "class_4": Highly relevant (score between 0.75 and 1.0)

                                Respond with only one of: "class_1", "class_2", "class_3", or "class_4".

                                Examples:

                                Search query: "How to reset a router to factory settings?"
                                Document chunk: "Computers often come with customizable parental control settings."
                                Score: class_1

                                Search query: "Symptoms of vitamin D deficiency"
                                Document chunk: "Vitamin D deficiency has been linked to fatigue, bone pain, and muscle weakness."
                                Score: class_4

                                Search query: "Best practices for onboarding remote employees"
                                Document chunk: "An employee handbook can be useful for new hires, outlining company policies and benefits."
                                Score: class_2

                                Search query: "Benefits of mindfulness meditation"
                                Document chunk: "Practicing mindfulness has shown to reduce stress and improve focus in multiple studies."
                                Score: class_3

                                Search query: "What is Kubernetes used for?"
                                Document chunk: "Kubernetes is an open-source system for automating deployment, scaling, and management of containerized applications."
                                Score: class_4

                                Search query: "How to bake sourdough bread at home"
                                Document chunk: "The French Revolution began in 1789 and radically transformed society."
                                Score: class_1

                                Search query: "Machine learning algorithms for image classification"
                                Document chunk: "Convolutional Neural Networks (CNNs) are particularly effective in image classification tasks."
                                Score: class_4

                                Search query: "How to improve focus while working remotely"
                                Document chunk: "Creating a dedicated workspace and setting a consistent schedule can significantly improve focus during remote work."
                                Score: class_4

                                Search query: "Carbon emissions from electric vehicles vs gas cars"
                                Document chunk: "Electric vehicles produce zero emissions while driving, but battery production has environmental impacts."
                                Score: class_3

                                Search query: "Time zones in the United States"
                                Document chunk: "The U.S. is divided into six primary time zones: Eastern, Central, Mountain, Pacific, Alaska, and Hawaii-Aleutian."
                                Score: class_4
                             """},

                            {"role": "user", "content": f"""
                                Now evaluate the following pair:

                                Search query: {query}
                                Document chunk: {document}

                                Which class best represents the relevance?
                            """}
                        ],
                        temperature=self.temperature,
                        n=1,
                        logprobs=True,
                        top_logprobs=4,
                        max_tokens=3
                    )

                    # Extract response and logprobs
                    class_label = response.choices[0].message.content.strip()
                    token_logprobs = response.choices[0].logprobs.content
                    # Reconstruct the prediction and extract the top logprobs from the final token (e.g., "1")
                    final_token_logprob = token_logprobs[-1]
                    top_logprobs = final_token_logprob.top_logprobs
                    # Create a map of 'class_1' -> probability, using token combinations
                    class_probs = {}
                    for top_token in top_logprobs:
                        full_label = f"class_{top_token.token}"
                        prob = math.exp(top_token.logprob)
                        class_probs[full_label] = prob
                    # Optional: normalize in case some are missing
                    total_prob = sum(class_probs.values())
                    class_probs = {k: v / total_prob for k, v in class_probs.items()}
                    # Assign weights to classes
                    class_weights = {
                        "class_1": 0.25,
                        "class_2": 0.5,
                        "class_3": 0.75,
                        "class_4": 1.0
                    }
                    # Compute the final smooth score
                    relevance_score = sum(class_weights.get(class_label, 0) * prob for class_label, prob in class_probs.items())
                    rerank_data = {
                        "document": document,
                        "answer": class_label,
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

    def get_scores(self, query: str, documents: list[str]):
        query_document_pairs = [(query, doc) for doc in documents]
        # Create event loop and run async code
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # If no running loop exists, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if self.method == "multi-class":  # default 'multi-class' method
            documents_and_scores = loop.run_until_complete(self._rank_score(query_document_pairs))
        else:
            documents_and_scores = loop.run_until_complete(self._rank(query_document_pairs))

        scores = [score for _, score in documents_and_scores]
        return scores
