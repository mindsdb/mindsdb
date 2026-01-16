from typing import List, Any
import json
import asyncio

import pandas as pd

from mindsdb.integrations.utilities.rag.retrievers.base import BaseRetriever, RunnableRetriever
from mindsdb.integrations.utilities.rag.utils import documents_to_df
from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator
from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class AutoRetriever(BaseRetriever):
    """
    AutoRetrieval is a class that uses LLM to extract metadata from documents and query vectorstore using self-query retrievers.
    """

    def __init__(
            self,
            config: RAGPipelineModel
    ):
        """

        :param config: RAGPipelineModel


        """

        self.documents = config.documents
        self.content_column_name = config.content_column_name
        self.vectorstore = config.vector_store
        self.filter_columns = config.auto_retriever_filter_columns
        self.document_description = config.dataset_description
        self.llm = config.llm
        self.embedding_model = config.embedding_model
        self.prompt_template = config.retriever_prompt_template
        self.cardinality_threshold = config.cardinality_threshold

    def _get_low_cardinality_columns(self, data: pd.DataFrame):
        """
        Given a dataframe, return a list of columns with low cardinality if datatype is not bool.
        :return:
        """
        low_cardinality_columns = []
        columns = data.columns if self.filter_columns is None else self.filter_columns
        for column in columns:
            if data[column].dtype != "bool":
                if data[column].nunique() < self.cardinality_threshold:
                    low_cardinality_columns.append(column)
        return low_cardinality_columns

    def get_metadata_field_info(self):
        """
        Given a list of Document, use llm to extract metadata from it.
        :return:
        """

        def _alter_description(data: pd.DataFrame,
                               low_cardinality_columns: list,
                               result: List[dict]):
            """
            For low cardinality columns, alter the description to include the sorted valid values.
            :param data: pd.DataFrame
            :param low_cardinality_columns: list
            :param result: List[dict]
            """
            for column_name in low_cardinality_columns:
                valid_values = sorted(data[column_name].unique())
                for entry in result:
                    if entry["name"] == column_name:
                        entry["description"] += f". Valid values: {valid_values}"

        data = documents_to_df(
            self.content_column_name,
            self.documents
        )

        prompt = self.prompt_template.format(dataframe=data.head().to_json(),
                                             description=self.document_description)
        # Call LLM and extract response
        llm_response = self.llm.invoke(prompt)
        # Extract content from LLM response
        if hasattr(llm_response, 'content'):
            response_text = llm_response.content
        elif isinstance(llm_response, str):
            response_text = llm_response
        else:
            response_text = str(llm_response)
        
        result: List[dict] = json.loads(response_text)

        _alter_description(
            data,
            self._get_low_cardinality_columns(data),
            result
        )

        return result

    def get_vectorstore(self):
        """

        :return:
        """
        return VectorStoreOperator(vector_store=self.vectorstore,
                                   documents=self.documents,
                                   embedding_model=self.embedding_model).vector_store

    def as_runnable(self) -> RunnableRetriever:
        """
        Return a custom self-query retriever
        :return: CustomSelfQueryRetriever instance
        """
        vectorstore = self.get_vectorstore()
        metadata_field_info = self.get_metadata_field_info()

        return CustomSelfQueryRetriever(
            llm=self.llm,
            vectorstore=vectorstore,
            document_contents=self.document_description,
            metadata_field_info=metadata_field_info
        )


class CustomSelfQueryRetriever:
    """
    Custom implementation of SelfQueryRetriever to replace langchain's SelfQueryRetriever.
    Uses LLM to generate metadata filters and queries vectorstore with those filters.
    """
    
    def __init__(
        self,
        llm: Any,
        vectorstore: Any,
        document_contents: str,
        metadata_field_info: List[dict]
    ):
        """
        Initialize CustomSelfQueryRetriever
        
        Args:
            llm: LLM instance with invoke method
            vectorstore: Vector store with similarity_search_with_score method
            document_contents: Description of document contents
            metadata_field_info: List of metadata field information dicts
        """
        self.llm = llm
        self.vectorstore = vectorstore
        self.document_contents = document_contents
        self.metadata_field_info = metadata_field_info
    
    def _generate_metadata_filters(self, query: str) -> dict:
        """
        Use LLM to generate metadata filters from query
        
        Args:
            query: User query string
            
        Returns:
            Dictionary of metadata filters
        """
        # Create prompt for LLM to generate metadata filters
        metadata_info_str = json.dumps(self.metadata_field_info, indent=2)
        prompt = f"""Given the following query and metadata field information, generate a structured query with metadata filters.

Query: {query}

Document contents description: {self.document_contents}

Available metadata fields:
{metadata_info_str}

Generate a JSON object with the query string and any applicable metadata filters. 
Format: {{"query": "extracted query", "filters": {{"field_name": "value"}}}}
"""
        
        try:
            llm_response = self.llm.invoke(prompt)
            # Extract content from LLM response
            if hasattr(llm_response, 'content'):
                response_text = llm_response.content
            elif isinstance(llm_response, str):
                response_text = llm_response
            else:
                response_text = str(llm_response)
            
            # Parse JSON response
            parsed = json.loads(response_text)
            return parsed.get('filters', {})
        except Exception as e:
            logger.warning(f"Error generating metadata filters: {e}")
            return {}
    
    def _query_vectorstore(self, query: str, filters: dict) -> List[Any]:
        """
        Query vectorstore with query and metadata filters
        
        Args:
            query: Query string
            filters: Metadata filters dictionary
            
        Returns:
            List of documents
        """
        # Use vectorstore's similarity_search method
        # If vectorstore supports metadata filtering, apply filters
        if hasattr(self.vectorstore, 'similarity_search'):
            # Try to pass filters if supported
            if filters:
                try:
                    # Some vectorstores support filter parameter
                    if hasattr(self.vectorstore, 'similarity_search_with_score'):
                        docs_with_scores = self.vectorstore.similarity_search_with_score(
                            query, k=4, filter=filters
                        )
                        return [doc for doc, _ in docs_with_scores]
                    else:
                        return self.vectorstore.similarity_search(query, k=4, filter=filters)
                except TypeError:
                    # If filter not supported, just do regular search
                    return self.vectorstore.similarity_search(query, k=4)
            else:
                return self.vectorstore.similarity_search(query, k=4)
        else:
            raise ValueError("Vectorstore must have similarity_search method")
    
    def invoke(self, query: str) -> List[Any]:
        """Sync invocation - retrieve documents for a query"""
        # Generate metadata filters
        filters = self._generate_metadata_filters(query)
        
        # Extract query string (LLM might have rewritten it)
        # For now, use original query
        # In a full implementation, we'd extract the rewritten query from LLM response
        
        # Query vectorstore
        return self._query_vectorstore(query, filters)
    
    async def ainvoke(self, query: str) -> List[Any]:
        """Async invocation - retrieve documents for a query"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.invoke, query)
    
    def get_relevant_documents(self, query: str) -> List[Any]:
        """Get relevant documents (sync)"""
        return self.invoke(query)
