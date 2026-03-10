from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Sequence

from mindsdb.integrations.utilities.rag.rerankers.base_reranker import BaseLLMReranker

log = logging.getLogger(__name__)


def _dispatch_custom_event(event_name: str, data: dict):
    """Simple event dispatcher replacement for langchain's dispatch_custom_event.

    This is a no-op implementation. If custom event handling is needed,
    it can be extended to dispatch events to registered handlers.
    """
    # No-op for now - can be extended if needed
    pass


class LLMReranker(BaseLLMReranker):
    remove_irrelevant: bool = True  # New flag to control removal of irrelevant documents

    def _dispatch_rerank_event(self, data):
        """Dispatch rerank event using custom event dispatcher"""
        _dispatch_custom_event("rerank", data)

    async def acompress_documents(
        self,
        documents: Sequence[Any],
        query: str,
        callbacks: Optional[Any] = None,
    ) -> Sequence[Any]:
        """
        Async compress documents using reranking with proper error handling.

        Args:
            documents: Sequence of document objects with page_content and metadata attributes
            query: Query string for reranking
            callbacks: Optional callbacks object with on_retriever_start, on_retriever_end,
                     on_text, and on_retriever_error methods

        Returns:
            Sequence of filtered and reranked documents
        """
        if callbacks and hasattr(callbacks, "on_retriever_start"):
            try:
                await callbacks.on_retriever_start({"query": query}, "Reranking documents")
            except Exception as e:
                log.warning(f"Error in callback on_retriever_start: {e}")

        log.info(f"Async compressing documents. Initial count: {len(documents)}")
        if not documents:
            if callbacks and hasattr(callbacks, "on_retriever_end"):
                try:
                    await callbacks.on_retriever_end({"documents": []})
                except Exception as e:
                    log.warning(f"Error in callback on_retriever_end: {e}")
            return []

        # Stream reranking update.
        _dispatch_custom_event("rerank_begin", {"num_documents": len(documents)})

        try:
            # Prepare query-document pairs
            # Use duck typing to access page_content attribute
            query_document_pairs = [(query, doc.page_content) for doc in documents]

            if callbacks and hasattr(callbacks, "on_text"):
                try:
                    await callbacks.on_text("Starting document reranking...")
                except Exception as e:
                    log.warning(f"Error in callback on_text: {e}")

            # Get ranked results
            ranked_results = await self._rank(query_document_pairs, rerank_callback=self._dispatch_rerank_event)

            # Sort by score in descending order
            ranked_results.sort(key=lambda x: x[1], reverse=True)

            # Filter based on threshold and num_docs_to_keep
            filtered_docs = []
            for doc, score in ranked_results:
                if score >= self.filtering_threshold:
                    matching_doc = next(d for d in documents if d.page_content == doc)
                    # Use duck typing to access and update metadata
                    metadata = getattr(matching_doc, "metadata", None) or {}
                    matching_doc.metadata = {**metadata, "relevance_score": score}
                    filtered_docs.append(matching_doc)

                    if callbacks and hasattr(callbacks, "on_text"):
                        try:
                            await callbacks.on_text(f"Document scored {score:.2f}")
                        except Exception as e:
                            log.warning(f"Error in callback on_text: {e}")

                    if self.num_docs_to_keep and len(filtered_docs) >= self.num_docs_to_keep:
                        break

            log.info(f"Async compression complete. Final count: {len(filtered_docs)}")

            if callbacks and hasattr(callbacks, "on_retriever_end"):
                try:
                    await callbacks.on_retriever_end({"documents": filtered_docs})
                except Exception as e:
                    log.warning(f"Error in callback on_retriever_end: {e}")

            return filtered_docs

        except Exception as e:
            error_msg = "Error during async document compression:"
            log.exception(error_msg)
            if callbacks and hasattr(callbacks, "on_retriever_error"):
                try:
                    await callbacks.on_retriever_error(f"{error_msg} {e}")
                except Exception as callback_error:
                    log.warning(f"Error in callback on_retriever_error: {callback_error}")
            return documents  # Return original documents on error

    def compress_documents(
        self,
        documents: Sequence[Any],
        query: str,
        callbacks: Optional[Any] = None,
    ) -> Sequence[Any]:
        """
        Sync wrapper for async compression.

        Args:
            documents: Sequence of document objects with page_content and metadata attributes
            query: Query string for reranking
            callbacks: Optional callbacks object

        Returns:
            Sequence of filtered and reranked documents
        """
        return asyncio.run(self.acompress_documents(documents, query, callbacks))

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        """Get the identifying parameters."""
        return {
            "model": self.model,
            "temperature": self.temperature,
            "remove_irrelevant": self.remove_irrelevant,
            "method": self.method,
        }
