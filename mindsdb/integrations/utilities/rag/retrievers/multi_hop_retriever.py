from typing import List, Any, Optional, Set
import json
import logging

from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import Field, PrivateAttr

logger = logging.getLogger(__name__)

DEFAULT_QUESTION_REFORMULATION_TEMPLATE = """Given the original question and the context retrieved, analyze what additional information we need to provide a complete answer.

Original Question: {question}

Retrieved Context:
{context}

Your task:
1. Analyze if the retrieved context fully answers the original question
2. If not, identify what additional information is needed
3. Formulate 1-3 specific follow-up questions that would help find the missing information
4. Format your response EXACTLY as a JSON list of questions

Example format:
["What specific military applications were developed for X?", "How did X impact Y during the time period?"]

If the context fully answers the question or no meaningful follow-up questions can be generated, return an empty list: []

Remember: Your response must be a valid JSON array of strings. Nothing else.

Follow-up Questions:"""


class MultiHopRetriever(BaseRetriever):
    """A retriever that implements multi-hop question reformulation strategy.

    This retriever enhances the standard retrieval process by:
    1. Making an initial query to find relevant documents
    2. Analyzing the retrieved context to identify information gaps
    3. Formulating follow-up questions to fill these gaps
    4. Making additional queries with these follow-up questions

    This is particularly useful for complex questions that require connecting
    information from multiple documents.

    Attributes:
        base_retriever: The underlying retriever to use for document retrieval
        llm: Language model to use for question reformulation
        reformulation_template: Template for prompting the LLM
        max_hops: Maximum number of follow-up questions to ask
    """

    base_retriever: BaseRetriever = Field(description="Base retriever to use for document retrieval")
    llm: Any = Field(description="Language model to use for question reformulation")
    reformulation_template: str = Field(default=DEFAULT_QUESTION_REFORMULATION_TEMPLATE)
    max_hops: int = Field(default=3, ge=1)

    _asked_questions: Set[str] = PrivateAttr(default_factory=set)

    def _get_relevant_documents(
        self, query: str, *, run_manager: Optional[Any] = None
    ) -> List[Document]:
        """Implement multi-hop retrieval strategy.

        Args:
            query: The initial question to answer
            run_manager: Optional callback manager for tracking progress

        Returns:
            List of relevant documents found through multiple hops
        """
        all_docs = []
        current_query = query
        hop_count = 0

        # Reset question memory for new query
        self._asked_questions = {query}

        logger.info(f"Starting multi-hop retrieval for query: {query}")

        while hop_count < self.max_hops:
            logger.debug(f"Hop {hop_count + 1}: Querying with '{current_query}'")

            # Get documents for current query
            current_docs = self.base_retriever._get_relevant_documents(current_query, run_manager=run_manager)

            if not current_docs:
                logger.debug(f"No documents found for query: {current_query}")
                break

            all_docs.extend(current_docs)
            logger.debug(f"Found {len(current_docs)} documents")

            # Format context for reformulation
            context = "\n\n".join(doc.page_content for doc in current_docs)

            # Generate prompt for follow-up questions
            prompt = self.reformulation_template.format(
                question=query,  # Original question
                context=context
            )

            # Get follow-up questions from LLM
            try:
                llm_output = self.llm.invoke(prompt, temperature=0.2)  # Low temperature for consistent outputs
                follow_up_questions = self._parse_follow_up_questions(llm_output)

                # Filter out previously asked questions
                follow_up_questions = [q for q in follow_up_questions if q not in self._asked_questions]

                if not follow_up_questions:
                    logger.debug("No new follow-up questions generated")
                    break

                # Use first follow-up question as next query
                current_query = follow_up_questions[0]
                self._asked_questions.add(current_query)

                logger.info(f"Generated follow-up question: {current_query}")
                hop_count += 1

            except Exception as e:
                logger.error(f"Error during question reformulation: {str(e)}")
                break

        logger.info(f"Multi-hop retrieval completed with {len(all_docs)} total documents")
        return all_docs

    def _parse_follow_up_questions(self, llm_output: str) -> List[str]:
        """Parse LLM output into list of questions.

        Args:
            llm_output: Raw output from the LLM

        Returns:
            List of parsed follow-up questions
        """
        try:
            # Clean the output and parse as JSON
            cleaned_output = llm_output.strip()
            if cleaned_output.startswith("[") and cleaned_output.endswith("]"):
                questions = json.loads(cleaned_output)
                # Validate that we got a list of strings
                if isinstance(questions, list) and all(isinstance(q, str) for q in questions):
                    return questions
            return []
        except Exception as e:
            logger.error(f"Error parsing LLM output: {str(e)}")
            return []
