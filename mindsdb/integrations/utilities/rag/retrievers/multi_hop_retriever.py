from typing import List, Optional

import json
from langchain_core.callbacks.manager import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.language_models import BaseChatModel
from langchain_core.retrievers import BaseRetriever
from pydantic import Field, PrivateAttr

from mindsdb.integrations.utilities.rag.settings import DEFAULT_QUESTION_REFORMULATION_TEMPLATE


class MultiHopRetriever(BaseRetriever):
    """A retriever that implements multi-hop question reformulation strategy.

    This retriever takes a base retriever and uses an LLM to generate follow-up
    questions based on the initial results. It then retrieves documents for each
    follow-up question and combines all results.
    """

    base_retriever: BaseRetriever = Field(description="Base retriever to use for document lookup")
    llm: BaseChatModel = Field(description="LLM to use for generating follow-up questions")
    max_hops: int = Field(default=3, description="Maximum number of follow-up questions to generate")
    reformulation_template: str = Field(
        default=DEFAULT_QUESTION_REFORMULATION_TEMPLATE,
        description="Template for reformulating questions"
    )

    _asked_questions: set = PrivateAttr(default_factory=set)

    def _get_relevant_documents(
        self, query: str, *, run_manager: Optional[CallbackManagerForRetrieverRun] = None
    ) -> List[Document]:
        """Get relevant documents using multi-hop retrieval."""
        if query in self._asked_questions:
            return []

        self._asked_questions.add(query)

        # Get initial documents
        docs = self.base_retriever._get_relevant_documents(query)
        if not docs or len(self._asked_questions) >= self.max_hops:
            return docs

        # Generate follow-up questions
        context = "\n".join(doc.page_content for doc in docs)
        prompt = self.reformulation_template.format(
            question=query,
            context=context
        )

        try:
            follow_up_questions = json.loads(self.llm.invoke(prompt))
            if not isinstance(follow_up_questions, list):
                return docs
        except (json.JSONDecodeError, TypeError):
            return docs

        # Get documents for follow-up questions
        for question in follow_up_questions:
            if isinstance(question, str):
                follow_up_docs = self._get_relevant_documents(question)
                docs.extend(follow_up_docs)

        return docs
