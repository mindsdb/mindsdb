from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Sequence

from langchain.retrievers.document_compressors.base import BaseDocumentCompressor
from langchain_core.callbacks import Callbacks, dispatch_custom_event
from langchain_core.documents import Document

from mindsdb.integrations.utilities.rag.rerankers.base_reranker import BaseLLMReranker

log = logging.getLogger(__name__)


class LLMReranker(BaseDocumentCompressor, BaseLLMReranker):
    remove_irrelevant: bool = True  # New flag to control removal of irrelevant documents

    def _dispatch_rerank_event(self, data):
        dispatch_custom_event("rerank", data)

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
            ranked_results = await self._rank(query_document_pairs, rerank_callback=self._dispatch_rerank_event)

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
            "method": self.method,
        }
