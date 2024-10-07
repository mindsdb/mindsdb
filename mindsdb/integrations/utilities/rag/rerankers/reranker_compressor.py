from __future__ import annotations

import asyncio
import logging
import math
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from langchain.chat_models import ChatOpenAI
from langchain.retrievers.document_compressors.base import BaseDocumentCompressor
from langchain.schema import Document
from langchain.schema import SystemMessage, HumanMessage
from langchain_core.callbacks import Callbacks
from pydantic import BaseModel

from mindsdb.integrations.utilities.rag.settings import DEFAULT_RERANKING_MODEL

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

    def _get_client(self) -> Any:
        """Ensure client is initialized and return it."""
        if not self.client:
            self._initialize_client()
        return self.client

    async def search_relevancy(self, query: str, document: str) -> Any:
        openai_api_key = self.openai_api_key or os.getenv(self._api_key_var)

        # Initialize the ChatOpenAI client
        client = ChatOpenAI(api_key=openai_api_key, model="gpt-4", temperature=0, logprobs=True)

        # Create the message history for the conversation
        message_history = [
            SystemMessage(
                content="""Your task is to classify whether the document is relevant to the search query provided below. Answer just "YES" or "NO"."""),
            HumanMessage(content=f"""Document: ```{document}```; Search query: ```{query}```""")
        ]

        # Generate the response using LangChain's chat model
        response = await client.agenerate(
            messages=[message_history],
            max_tokens=1
        )

        # Return the response from the model
        return response.generations[0]

    async def _rank(self, query_document_pairs: List[Tuple[str, str]]) -> List[Tuple[str, float]]:
        # Gather results asynchronously for all query-document pairs
        results = await asyncio.gather(
            *[self.search_relevancy(query=query, document=document) for (query, document) in query_document_pairs]
        )

        ranked_results = []

        for idx, result in enumerate(results):
            # Extract the log probability (assuming logprobs are provided in LangChain response)
            msg = result[0].message
            logprob = msg.response_metadata['logprobs']['content'][0]['logprob']
            prob = math.exp(logprob)
            answer = result[0].message.content  # The model's "YES" or "NO" response

            # Calculate the score based on the model's response
            if answer == "YES":
                score = prob
            elif answer == "NO":
                score = 1 - prob
            else:
                score = 0.0  # Default if something unexpected happens

            # Append the document and score to the result
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
        if len(documents) == 0:
            log.warning("No documents to compress. Returning empty list.")
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
            "temperature": self.temperature,
            "remove_irrelevant": self.remove_irrelevant,
        }
