from __future__ import annotations

import re
import json
import asyncio
import logging
import math
import os
import random
from abc import ABC
from typing import Any, List, Optional, Tuple

from openai import AsyncOpenAI, AsyncAzureOpenAI
from pydantic import BaseModel

from mindsdb.integrations.utilities.rag.settings import (
    DEFAULT_RERANKING_MODEL,
    DEFAULT_LLM_ENDPOINT,
    DEFAULT_RERANKER_N,
    DEFAULT_RERANKER_LOGPROBS,
    DEFAULT_RERANKER_TOP_LOGPROBS,
    DEFAULT_RERANKER_MAX_TOKENS,
    DEFAULT_VALID_CLASS_TOKENS,
    RerankerMode,
)
from mindsdb.integrations.libs.base import BaseMLEngine

log = logging.getLogger(__name__)


def get_event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # If no running loop exists, create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


class BaseLLMReranker(BaseModel, ABC):
    filtering_threshold: float = 0.0  # Default threshold for filtering
    provider: str = "openai"
    model: str = DEFAULT_RERANKING_MODEL  # Model to use for reranking
    temperature: float = 0.0  # Temperature for the model
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    api_version: Optional[str] = None
    num_docs_to_keep: Optional[int] = None  # How many of the top documents to keep after reranking & compressing.
    method: str = "multi-class"  # Scoring method: 'multi-class' or 'binary'
    mode: RerankerMode = RerankerMode.POINTWISE
    _api_key_var: str = "OPENAI_API_KEY"
    client: Optional[AsyncOpenAI | BaseMLEngine] = None
    _semaphore: Optional[asyncio.Semaphore] = None
    max_concurrent_requests: int = 20
    max_retries: int = 4
    retry_delay: float = 1.0
    request_timeout: float = 20.0  # Timeout for API requests
    early_stop: bool = True  # Whether to enable early stopping
    early_stop_threshold: float = 0.8  # Confidence threshold for early stopping
    n: int = DEFAULT_RERANKER_N  # Number of completions to generate
    logprobs: bool = DEFAULT_RERANKER_LOGPROBS  # Whether to include log probabilities
    top_logprobs: int = DEFAULT_RERANKER_TOP_LOGPROBS  # Number of top log probabilities to include
    max_tokens: int = DEFAULT_RERANKER_MAX_TOKENS  # Maximum tokens to generate
    valid_class_tokens: List[str] = DEFAULT_VALID_CLASS_TOKENS

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self._init_client()

    def _init_client(self):
        if self.client is None:
            if self.provider == "azure_openai":
                azure_api_key = self.api_key or os.getenv("AZURE_OPENAI_API_KEY")
                azure_api_endpoint = self.base_url or os.environ.get("AZURE_OPENAI_ENDPOINT")
                azure_api_version = self.api_version or os.environ.get("AZURE_OPENAI_API_VERSION")
                self.client = AsyncAzureOpenAI(
                    api_key=azure_api_key,
                    azure_endpoint=azure_api_endpoint,
                    api_version=azure_api_version,
                    timeout=self.request_timeout,
                    max_retries=2,
                )
            elif self.provider in ("openai", "ollama"):
                if self.provider == "ollama":
                    self.method = "no-logprobs"
                    if self.api_key is None:
                        self.api_key = "n/a"

                api_key_var: str = "OPENAI_API_KEY"
                openai_api_key = self.api_key or os.getenv(api_key_var)
                if not openai_api_key:
                    raise ValueError(f"OpenAI API key not found in environment variable {api_key_var}")

                base_url = self.base_url or DEFAULT_LLM_ENDPOINT
                self.client = AsyncOpenAI(
                    api_key=openai_api_key, base_url=base_url, timeout=self.request_timeout, max_retries=2
                )
            else:
                # try to use litellm
                from mindsdb.api.executor.controllers.session_controller import SessionController

                session = SessionController()
                module = session.integration_controller.get_handler_module("litellm")

                if module is None or module.Handler is None:
                    raise ValueError(f'Unable to use "{self.provider}" provider. Litellm handler is not installed')

                self.client = module.Handler
                self.method = "no-logprobs"

    async def _call_llm(self, messages):
        if self.provider in ("azure_openai", "openai", "ollama"):
            return await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
            )
        else:
            kwargs = self.model_extra.copy()

            if self.base_url is not None:
                kwargs["api_base"] = self.base_url

            if self.api_key is not None:
                kwargs["api_key"] = self.api_key

            return await self.client.acompletion(self.provider, model=self.model, messages=messages, args=kwargs)

    async def _rank(self, query_document_pairs: List[Tuple[str, str]], rerank_callback=None) -> List[Tuple[str, float]]:
        ranked_results = []

        # Process in larger batches for better throughput
        batch_size = min(self.max_concurrent_requests * 2, len(query_document_pairs))
        for i in range(0, len(query_document_pairs), batch_size):
            batch = query_document_pairs[i : i + batch_size]

            results = await asyncio.gather(
                *[
                    self._backoff_wrapper(query=query, document=document, rerank_callback=rerank_callback)
                    for (query, document) in batch
                ],
                return_exceptions=True,
            )

            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    log.error(f"Error processing document {i + idx}: {str(result)}")
                    raise RuntimeError(f"Error during reranking: {result}") from result

                score = result["relevance_score"]

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
                    log.warning(f"Error in early stopping check: {e}")

        return ranked_results

    async def _backoff_wrapper(self, query: str, document: str, rerank_callback=None) -> Any:
        async with self._semaphore:
            for attempt in range(self.max_retries):
                try:
                    if self.method == "multi-class":
                        rerank_data = await self.search_relevancy_score(query, document)
                    elif self.method == "no-logprobs":
                        rerank_data = await self.search_relevancy_no_logprob(query, document)
                    else:
                        rerank_data = await self.search_relevancy(query, document)
                    if rerank_callback is not None:
                        rerank_callback(rerank_data)
                    return rerank_data

                except Exception as e:
                    if attempt == self.max_retries - 1:
                        log.error(f"Failed after {self.max_retries} attempts: {str(e)}")
                        raise
                    # Exponential backoff with jitter
                    retry_delay = self.retry_delay * (2**attempt) + random.uniform(0, 0.1)
                    await asyncio.sleep(retry_delay)

    async def search_relevancy(self, query: str, document: str) -> Any:
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": "Rate the relevance of the document to the query. Respond with 'yes' or 'no'.",
                },
                {"role": "user", "content": f"Query: {query}\nDocument: {document}\nIs this document relevant?"},
            ],
            temperature=self.temperature,
            n=1,
            logprobs=True,
            max_tokens=1,
        )

        # Extract response and logprobs
        answer = response.choices[0].message.content
        logprob = response.choices[0].logprobs.content[0].logprob

        # Convert answer to score using the model's confidence
        if answer.lower().strip() == "yes":
            score = logprob  # If yes, use the model's confidence
        elif answer.lower().strip() == "no":
            score = 1 - logprob  # If no, invert the confidence
        else:
            score = 0.5 * logprob  # For unclear answers, reduce confidence

        rerank_data = {
            "document": document,
            "relevance_score": score,
        }

        return rerank_data

    async def search_relevancy_no_logprob(self, query: str, document: str) -> Any:
        prompt = (
            f"Score the relevance between search query and user message on scale between 0 and 100 per cents. "
            f"Consider semantic meaning, key concepts, and contextual relevance. "
            f"Return ONLY a numerical score between 0 and 100 per cents. No other text. Stop after sending a number. "
            f"Search query: {query}"
        )

        response = await self._call_llm(
            messages=[{"role": "system", "content": prompt}, {"role": "user", "content": document}],
        )

        answer = response.choices[0].message.content

        try:
            value = re.findall(r"[\d]+", answer)[0]
            score = float(value) / 100
            score = max(0.0, min(score, 1.0))
        except (ValueError, IndexError):
            score = 0.0

        rerank_data = {
            "document": document,
            "relevance_score": score,
        }

        return rerank_data

    async def search_relevancy_score(self, query: str, document: str) -> Any:
        """
        This method is used to score the relevance of a document to a query.

        Args:
            query: The query to score the relevance of.
            document: The document to score the relevance of.

        Returns:
            A dictionary with the document and the relevance score.
        """

        log.debug("Start search_relevancy_score")
        log.debug(f"Reranker query: {query[:5]}")
        log.debug(f"Reranker document: {document[:50]}")
        log.debug(f"Reranker model: {self.model}")
        log.debug(f"Reranker temperature: {self.temperature}")
        log.debug(f"Reranker n: {self.n}")
        log.debug(f"Reranker logprobs: {self.logprobs}")
        log.debug(f"Reranker top_logprobs: {self.top_logprobs}")
        log.debug(f"Reranker max_tokens: {self.max_tokens}")
        log.debug(f"Reranker valid_class_tokens: {self.valid_class_tokens}")

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": """
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
                 """,
                },
                {
                    "role": "user",
                    "content": f"""
                    Now evaluate the following pair:

                    Search query: {query}
                    Document chunk: {document}

                    Which class best represents the relevance?
                """,
                },
            ],
            temperature=self.temperature,
            n=self.n,
            logprobs=self.logprobs,
            top_logprobs=self.top_logprobs,
            max_tokens=self.max_tokens,
        )

        # Extract response and logprobs
        token_logprobs = response.choices[0].logprobs.content

        # Find the token that contains the class number
        # Instead of just taking the last token, search for the actual class number token
        class_token_logprob = None
        for token_logprob in reversed(token_logprobs):
            if token_logprob.token in self.valid_class_tokens:
                class_token_logprob = token_logprob
                break

        # If we couldn't find a class token, fall back to the last non-empty token
        if class_token_logprob is None:
            log.warning("No class token logprob found, using the last token as fallback")
            class_token_logprob = token_logprobs[-1]

        top_logprobs = class_token_logprob.top_logprobs

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
        class_weights = {"class_1": 0.25, "class_2": 0.5, "class_3": 0.75, "class_4": 1.0}
        # Compute the final smooth score
        score = sum(class_weights.get(class_label, 0) * prob for class_label, prob in class_probs.items())
        if score is not None:
            if score > 1.0:
                score = 1.0
            elif score < 0.0:
                score = 0.0

        rerank_data = {"document": document, "relevance_score": score}
        log.debug(f"Reranker score: {score}")
        log.debug("End search_relevancy_score")
        return rerank_data

    def get_scores(self, query: str, documents: list[str]):
        query_document_pairs = [(query, doc) for doc in documents]
        # Create event loop and run async code

        documents_and_scores = get_event_loop().run_until_complete(self._rank(query_document_pairs))

        scores = [score for _, score in documents_and_scores]
        return scores


def _strip_code_fences(text: str) -> str:
    """Strip code fences from text, handling cases where first line has content after fence."""
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        # Check if first line has content after the fence (e.g., ```json)
        first_line = lines[0] if lines else ""
        if first_line.strip() == "```" or (first_line.startswith("```") and len(first_line.strip()) > 3):
            # Drop first fence line (with or without language specifier)
            lines = lines[1:]
        # Drop trailing fence lines
        while lines and lines[-1].strip().startswith("```"):
            lines.pop()
        stripped = "\n".join(lines).strip()
    return stripped


class ListwiseLLMReranker(BaseLLMReranker):
    mode: RerankerMode = RerankerMode.LISTWISE
    max_document_characters: int = 3000
    max_documents_per_batch: int = 50  # Maximum documents to rank in a single LLM call
    document_separator: str = "\n---DOCUMENT_SEPARATOR---\n"  # Unique separator to avoid conflicts

    async def _rank(self, query_document_pairs: List[Tuple[str, str]], rerank_callback=None) -> List[Tuple[str, float]]:
        if not query_document_pairs:
            return []

        query = query_document_pairs[0][0]
        documents = [document for _, document in query_document_pairs]

        # Handle large document sets by batching
        if len(documents) > self.max_documents_per_batch:
            log.info(f"Batching {len(documents)} documents into groups of {self.max_documents_per_batch}")
            return await self._rank_with_batching(query, documents, rerank_callback)

        # Use _rank_single_batch for consistency
        return await self._rank_single_batch(query_document_pairs, rerank_callback)

    async def _rank_with_batching(
        self, query: str, documents: List[str], rerank_callback=None
    ) -> List[Tuple[str, float]]:
        """Rank documents in batches to avoid overwhelming the LLM with too many documents."""
        batch_size = self.max_documents_per_batch
        num_batches = (len(documents) + batch_size - 1) // batch_size

        all_results: List[Tuple[str, float]] = []

        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(documents))
            batch_docs = documents[start_idx:end_idx]

            # Create query-document pairs for this batch
            batch_pairs = [(query, doc) for doc in batch_docs]

            # Rank this batch
            batch_results = await self._rank_single_batch(batch_pairs, rerank_callback)
            all_results.extend(batch_results)

        # Sort all results by score to get final ranking
        all_results.sort(key=lambda item: item[1], reverse=True)
        return all_results

    async def _rank_single_batch(
        self, query_document_pairs: List[Tuple[str, str]], rerank_callback=None
    ) -> List[Tuple[str, float]]:
        """Rank a single batch of documents."""
        query = query_document_pairs[0][0]
        documents = [document for _, document in query_document_pairs]

        messages = self._build_messages(query, documents)

        for attempt in range(self.max_retries):
            try:
                response = await self._call_llm(messages)
                content = response.choices[0].message.content
                scores = self._extract_scores(content, len(documents))
                return list(zip(documents, scores))
            except Exception as exc:
                if attempt == self.max_retries - 1:
                    log.error(f"Failed listwise reranking batch after {self.max_retries} attempts: {exc}")
                    raise
                retry_delay = self.retry_delay * (2**attempt) + random.uniform(0, 0.1)
                await asyncio.sleep(retry_delay)

        return []

    def _build_messages(self, query: str, documents: List[str]) -> List[dict]:
        document_blocks = []
        for idx, document in enumerate(documents, start=1):
            # Remove any existing 'Document [N]:' prefix from content
            cleaned_doc = self._clean_document_prefix(document)
            truncated = self._truncate_document(cleaned_doc)
            document_blocks.append(f"Document {idx}:\n{truncated}")

        docs_text = self.document_separator.join(document_blocks)
        system_prompt = (
            "You are an expert reranker. Given a user query and a list of candidate "
            "documents, you must rank the documents from most to least relevant. "
            'Only respond with JSON following the schema: {"ranking": ['
            '{"doc_index": <1-based document index>, "score": <float between 0 and 1>}]}.'
        )

        user_prompt = (
            f"""
            Query:
            {query}

            Documents:
            {docs_text}

            Return the ranking as JSON. Make sure every document appears once. Scores must be between 0 and 1.
            """
        ).strip()

        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

    def _clean_document_prefix(self, document: str) -> str:
        """Remove 'Document [N]:' prefix if present in the document content."""
        pattern = r"^Document\s+\d+:\s*"
        return re.sub(pattern, "", document, count=1)

    def _truncate_document(self, document: str) -> str:
        if len(document) <= self.max_document_characters:
            return document
        return document[: self.max_document_characters] + "..."

    def _extract_scores(self, content: str, num_documents: int) -> List[float]:
        sanitized = _strip_code_fences(content)
        fallback_scores = self._fallback_scores(num_documents)
        parsed_scores = fallback_scores.copy()

        try:
            parsed = json.loads(sanitized)
        except json.JSONDecodeError as exc:
            log.warning(f"Failed to parse listwise reranker response as JSON: {exc}. Using fallback scores.")
            return parsed_scores

        ranking = parsed.get("ranking", []) if isinstance(parsed, dict) else parsed
        if not isinstance(ranking, list):
            log.warning("Listwise reranker response missing 'ranking' list. Using fallback scores.")
            return parsed_scores

        assignment_order = 0
        assigned: dict[int, float] = {}

        for rank_position, entry in enumerate(ranking):
            doc_index: Optional[int] = None
            score: Optional[float] = None

            if isinstance(entry, dict):
                doc_index = entry.get("doc_index")
                score = entry.get("score")
            elif isinstance(entry, (list, tuple)) and entry:
                doc_index = entry[0]
                if len(entry) > 1:
                    score = entry[1]
            elif isinstance(entry, int):
                doc_index = entry

            if doc_index is None:
                continue

            if isinstance(doc_index, str) and doc_index.isdigit():
                doc_index = int(doc_index)

            if not isinstance(doc_index, int):
                continue

            # Accept either 0-based or 1-based indices
            if doc_index <= 0:
                adjusted_index = doc_index
            else:
                adjusted_index = doc_index - 1

            if adjusted_index < 0 or adjusted_index >= num_documents:
                continue

            normalized_score = self._normalize_score(score)
            if normalized_score is None:
                normalized_score = fallback_scores[min(rank_position, num_documents - 1)]

            assigned[adjusted_index] = normalized_score
            assignment_order = max(assignment_order, rank_position + 1)

        next_rank = assignment_order
        for doc_idx in range(num_documents):
            if doc_idx in assigned:
                parsed_scores[doc_idx] = assigned[doc_idx]
            else:
                parsed_scores[doc_idx] = fallback_scores[min(next_rank, num_documents - 1)]
                next_rank += 1

        return parsed_scores

    def _normalize_score(self, score: Any) -> Optional[float]:
        if score is None:
            return None
        try:
            value = float(score)
        except (TypeError, ValueError):
            return None

        if math.isnan(value) or math.isinf(value):
            return None

        if value > 1:
            value = 1.0
        elif value < 0:
            value = 0.0

        return value

    def _fallback_scores(self, length: int) -> List[float]:
        if length <= 0:
            return []
        return [max(0.0, (length - idx) / length) for idx in range(length)]
