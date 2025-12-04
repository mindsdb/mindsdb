from typing import List, Optional, Any

import json
from pydantic import Field, PrivateAttr

from mindsdb.integrations.utilities.rag.settings import (
    RAGPipelineModel,
    DEFAULT_QUESTION_REFORMULATION_TEMPLATE
)
from mindsdb.integrations.utilities.rag.retrievers.retriever_factory import create_retriever
from mindsdb.integrations.utilities.rag.retrievers.base import BaseRetriever, RunnableRetriever
from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MultiHopRetriever(BaseRetriever):
    """A retriever that implements multi-hop question reformulation strategy.

    This retriever takes a base retriever and uses an LLM to generate follow-up
    questions based on the initial results. It then retrieves documents for each
    follow-up question and combines all results.
    """

    base_retriever: Any = Field(description="Base retriever to use for document lookup (must have get_relevant_documents or invoke method)")
    llm: Any = Field(description="LLM to use for generating follow-up questions (must have invoke method)")
    max_hops: int = Field(default=3, description="Maximum number of follow-up questions to generate")
    reformulation_template: str = Field(
        default=DEFAULT_QUESTION_REFORMULATION_TEMPLATE,
        description="Template for reformulating questions"
    )

    _asked_questions: set = PrivateAttr(default_factory=set)

    @classmethod
    def from_config(cls, config: RAGPipelineModel) -> "MultiHopRetriever":
        """Create a MultiHopRetriever from a RAGPipelineModel config."""
        if config.multi_hop_config is None:
            raise ValueError("multi_hop_config must be set for MultiHopRetriever")

        # Create base retriever based on type
        base_retriever = create_retriever(config, config.multi_hop_config.base_retriever_type)

        return cls(
            base_retriever=base_retriever,
            llm=config.llm,
            max_hops=config.multi_hop_config.max_hops,
            reformulation_template=config.multi_hop_config.reformulation_template
        )

    def _get_relevant_documents(
        self, query: str, *, run_manager: Optional[Any] = None
    ) -> List[Any]:
        """
        Get relevant documents using multi-hop retrieval.
        
        Args:
            query: Query string
            run_manager: Optional callback manager (not used, kept for compatibility)
        
        Returns:
            List of documents with page_content and metadata attributes
        """
        if query in self._asked_questions:
            return []

        self._asked_questions.add(query)

        # Get initial documents using duck typing
        docs = self._retrieve_from_base_retriever(query)
        if not docs or len(self._asked_questions) >= self.max_hops:
            return docs

        # Generate follow-up questions
        context = "\n".join(doc.page_content if hasattr(doc, 'page_content') else str(doc) for doc in docs)
        prompt = self.reformulation_template.format(
            question=query,
            context=context
        )

        try:
            # Call LLM - handle both string and message formats
            llm_response = self.llm.invoke(prompt)
            # Extract content from LLM response
            if hasattr(llm_response, 'content'):
                response_text = llm_response.content
            elif isinstance(llm_response, str):
                response_text = llm_response
            else:
                response_text = str(llm_response)
            
            follow_up_questions = json.loads(response_text)
            if not isinstance(follow_up_questions, list):
                return docs
        except (json.JSONDecodeError, TypeError, Exception) as e:
            logger.warning(f"Error parsing follow-up questions: {e}")
            return docs

        # Get documents for follow-up questions
        for question in follow_up_questions:
            if isinstance(question, str):
                follow_up_docs = self._get_relevant_documents(question)
                docs.extend(follow_up_docs)

        return docs
    
    def _retrieve_from_base_retriever(self, query: str) -> List[Any]:
        """Retrieve documents from base retriever using duck typing"""
        if hasattr(self.base_retriever, '_get_relevant_documents'):
            return self.base_retriever._get_relevant_documents(query)
        elif hasattr(self.base_retriever, 'get_relevant_documents'):
            return self.base_retriever.get_relevant_documents(query)
        elif hasattr(self.base_retriever, 'invoke'):
            return self.base_retriever.invoke(query)
        else:
            raise ValueError("Base retriever must have _get_relevant_documents, get_relevant_documents, or invoke method")
    
    def invoke(self, query: str) -> List[Any]:
        """Sync invocation - retrieve documents for a query"""
        return self._get_relevant_documents(query)
    
    async def ainvoke(self, query: str) -> List[Any]:
        """Async invocation - retrieve documents for a query"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_relevant_documents, query)
    
    def get_relevant_documents(self, query: str) -> List[Any]:
        """Get relevant documents (sync)"""
        return self._get_relevant_documents(query)
    
    def as_runnable(self) -> RunnableRetriever:
        """Return self as a runnable retriever"""
        return self
