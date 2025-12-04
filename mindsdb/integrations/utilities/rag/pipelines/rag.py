from copy import copy
from typing import Optional, Any, List, Union
import asyncio

from mindsdb.interfaces.knowledge_base.embedding_model_utils import construct_embedding_model_from_args
from mindsdb.integrations.libs.vectordatabase_handler import DistanceFunction
from mindsdb.integrations.utilities.rag.chains.map_reduce_summarizer_chain import MapReduceSummarizerChain
from mindsdb.integrations.utilities.rag.retrievers.auto_retriever import AutoRetriever
from mindsdb.integrations.utilities.rag.retrievers.multi_vector_retriever import MultiVectorRetriever
from mindsdb.integrations.utilities.rag.retrievers.sql_retriever import SQLRetriever
from mindsdb.integrations.utilities.rag.rerankers.reranker_compressor import LLMReranker
from mindsdb.integrations.utilities.rag.settings import (RAGPipelineModel,
                                                         DEFAULT_AUTO_META_PROMPT_TEMPLATE,
                                                         SearchKwargs, SearchType,
                                                         RerankerConfig,
                                                         SummarizationConfig, VectorStoreConfig)
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RERANKER_FLAG

from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator
from mindsdb.interfaces.knowledge_base.llm_wrapper import create_chat_model
from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SimpleRAGPipeline:
    """
    Custom RAG pipeline implementation to replace LangChain LCEL components
    """
    
    def __init__(
        self,
        retriever_runnable: Any,
        prompt_template: str,
        llm: Any,
        reranker: Optional[Any] = None,
        summarizer: Optional[Any] = None,
    ):
        """
        Initialize SimpleRAGPipeline
        
        Args:
            retriever_runnable: Retriever that can be invoked with question
            prompt_template: Prompt template string with {question} and {context} placeholders
            llm: Language model with invoke/ainvoke methods
            reranker: Optional reranker for document reranking
            summarizer: Optional summarizer chain
        """
        self.retriever_runnable = retriever_runnable
        self.prompt_template = prompt_template
        self.llm = llm
        self.reranker = reranker
        self.summarizer = summarizer
    
    def _format_docs(self, docs: Union[List[Any], str]) -> str:
        """Format documents into context string"""
        if isinstance(docs, str):
            # Handle case where retriever returns a string (e.g., SQLRetriever)
            return docs
        if not docs:
            return ''
        
        # Sort by original document so we can group source summaries together
        docs.sort(key=lambda d: d.metadata.get('original_row_id') if hasattr(d, 'metadata') and d.metadata else 0)
        original_document_id = None
        summary_prepended_text = 'Summary of the original document that the below context was taken from:\n'
        document_content = ''
        
        for d in docs:
            metadata = d.metadata if hasattr(d, 'metadata') else {}
            if metadata.get('original_row_id') != original_document_id and metadata.get('summary'):
                # We have a summary of a new document to prepend
                original_document_id = metadata.get('original_row_id')
                summary = f"{summary_prepended_text}{metadata.get('summary')}\n"
                document_content += summary
            
            page_content = d.page_content if hasattr(d, 'page_content') else str(d)
            document_content += f'{page_content}\n\n'
        
        return document_content
    
    def _format_prompt(self, question: str, context: str) -> str:
        """Format prompt template with question and context"""
        return self.prompt_template.format(question=question, context=context)
    
    def _extract_llm_response(self, response: Any) -> str:
        """Extract text content from LLM response"""
        # Handle different response types
        if isinstance(response, str):
            return response
        if hasattr(response, 'content'):
            return response.content
        if hasattr(response, 'text'):
            return response.text
        # Try to get from message if it's a message object
        if hasattr(response, 'message') and hasattr(response.message, 'content'):
            return response.message.content
        # Fallback to string conversion
        return str(response)
    
    async def _retrieve_documents(self, question: str) -> List[Any]:
        """Retrieve documents using retriever"""
        # Try async first
        if hasattr(self.retriever_runnable, 'ainvoke'):
            return await self.retriever_runnable.ainvoke(question)
        elif hasattr(self.retriever_runnable, 'invoke'):
            return self.retriever_runnable.invoke(question)
        elif hasattr(self.retriever_runnable, 'get_relevant_documents'):
            # Sync method, run in executor for async compatibility
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.retriever_runnable.get_relevant_documents, question)
        else:
            raise ValueError("Retriever must have ainvoke, invoke, or get_relevant_documents method")
    
    async def ainvoke(self, question: Union[str, dict]) -> dict:
        """Async invocation of the RAG pipeline"""
        # Handle both string and dict input (for compatibility)
        if isinstance(question, dict):
            question = question.get('question', question.get('input', ''))
        
        # 1. Retrieve documents
        docs = await self._retrieve_documents(question)
        
        # 2. Apply reranker if enabled
        if self.reranker and docs:
            try:
                # Convert to langchain Document format if needed for reranker
                from langchain_core.documents import Document as LangchainDocument
                langchain_docs = []
                for doc in docs:
                    if isinstance(doc, SimpleDocument):
                        langchain_docs.append(LangchainDocument(
                            page_content=doc.page_content,
                            metadata=doc.metadata
                        ))
                    elif hasattr(doc, 'page_content'):
                        langchain_docs.append(doc)
                    else:
                        langchain_docs.append(LangchainDocument(page_content=str(doc)))
                
                if langchain_docs:
                    docs = await self.reranker.acompress_documents(langchain_docs, question)
                    # Convert back to SimpleDocument if needed
                    simple_docs = []
                    for doc in docs:
                        if isinstance(doc, SimpleDocument):
                            simple_docs.append(doc)
                        else:
                            simple_docs.append(SimpleDocument(
                                page_content=doc.page_content if hasattr(doc, 'page_content') else str(doc),
                                metadata=doc.metadata if hasattr(doc, 'metadata') else {}
                            ))
                    docs = simple_docs
            except Exception as e:
                logger.warning(f"Error during reranking, continuing without reranking: {e}")
        
        # 3. Apply summarizer if enabled
        if self.summarizer and docs:
            try:
                # Summarizer expects dict with 'context' and 'question' keys
                summarizer_input = {
                    'context': docs,
                    'question': question
                }
                if hasattr(self.summarizer, 'ainvoke'):
                    summarizer_result = await self.summarizer.ainvoke(summarizer_input)
                elif hasattr(self.summarizer, 'invoke'):
                    summarizer_result = self.summarizer.invoke(summarizer_input)
                else:
                    summarizer_result = summarizer_input
                
                # Extract context from summarizer result
                if isinstance(summarizer_result, dict):
                    docs = summarizer_result.get('context', docs)
                else:
                    docs = summarizer_result
            except Exception as e:
                logger.warning(f"Error during summarization, continuing without summarization: {e}")
        
        # 4. Format documents into context
        context = self._format_docs(docs)
        
        # 5. Format prompt
        formatted_prompt = self._format_prompt(question, context)
        
        # 6. Generate answer using LLM
        # Langchain LLMs can accept strings (converted to HumanMessage) or messages
        # Try to use messages format if LLM supports it, otherwise use string
        try:
            # Try to import HumanMessage to create proper message format
            from langchain_core.messages import HumanMessage
            messages = [HumanMessage(content=formatted_prompt)]
        except ImportError:
            # Fallback to string if langchain not available
            messages = formatted_prompt
        
        if hasattr(self.llm, 'ainvoke'):
            llm_response = await self.llm.ainvoke(messages)
        elif hasattr(self.llm, 'invoke'):
            loop = asyncio.get_event_loop()
            llm_response = await loop.run_in_executor(None, self.llm.invoke, messages)
        else:
            raise ValueError("LLM must have ainvoke or invoke method")
        
        # 7. Extract text from LLM response
        answer = self._extract_llm_response(llm_response)
        
        # 8. Return dict with context, question, answer
        return {
            'context': docs,
            'question': question,
            'answer': answer
        }
    
    def invoke(self, question: Union[str, dict]) -> dict:
        """Sync invocation of the RAG pipeline"""
        return asyncio.run(self.ainvoke(question))


class LangChainRAGPipeline:
    """
    Builds a RAG pipeline using langchain LCEL components

    Args:
        retriever_runnable: Base retriever component
        prompt_template: Template for generating responses
        llm: Language model for generating responses
        reranker (bool): Whether to use reranking (default: False)
        reranker_config (RerankerConfig): Configuration for the reranker, including:
            - model: Model to use for reranking
            - filtering_threshold: Minimum score to keep a document
            - num_docs_to_keep: Maximum number of documents to keep
            - max_concurrent_requests: Maximum concurrent API requests
            - max_retries: Number of retry attempts for failed requests
            - retry_delay: Delay between retries
            - early_stop (bool): Whether to enable early stopping
            - early_stop_threshold: Confidence threshold for early stopping
        vector_store_config (VectorStoreConfig): Vector store configuration
        summarization_config (SummarizationConfig): Summarization configuration
    """

    def __init__(
            self,
            retriever_runnable,
            prompt_template,
            llm,
            reranker: bool = DEFAULT_RERANKER_FLAG,
            reranker_config: Optional[RerankerConfig] = None,
            vector_store_config: Optional[VectorStoreConfig] = None,
            summarization_config: Optional[SummarizationConfig] = None
    ):
        self.retriever_runnable = retriever_runnable
        self.prompt_template = prompt_template
        self.llm = llm
        if reranker:
            if reranker_config is None:
                reranker_config = RerankerConfig()
            # Convert config to dict and initialize reranker
            reranker_kwargs = reranker_config.model_dump(exclude_none=True)
            self.reranker = LLMReranker(**reranker_kwargs)
        else:
            self.reranker = None
        self.summarizer = None
        self.vector_store_config = vector_store_config
        knowledge_base_table = self.vector_store_config.kb_table if self.vector_store_config is not None else None
        if summarization_config is not None and knowledge_base_table is not None:
            self.summarizer = MapReduceSummarizerChain(
                vector_store_handler=knowledge_base_table.get_vector_db(),
                table_name=knowledge_base_table.get_vector_db_table_name(),
                summarization_config=summarization_config
            )

    def with_returned_sources(self) -> SimpleRAGPipeline:
        """
        Builds a RAG pipeline with returned sources
        :return: SimpleRAGPipeline instance
        """
        # Ensure all the required components are not None
        if self.prompt_template is None:
            raise ValueError("One of the required components (prompt_template) is None")
        if self.llm is None:
            raise ValueError("One of the required components (llm) is None")

        # Return SimpleRAGPipeline instance that handles all the pipeline logic
        return SimpleRAGPipeline(
            retriever_runnable=self.retriever_runnable,
            prompt_template=self.prompt_template,
            llm=self.llm,
            reranker=self.reranker,
            summarizer=self.summarizer
        )

    async def ainvoke(self, input_dict: dict) -> dict:
        """Async invocation of the RAG pipeline."""
        chain = self.with_returned_sources()
        return await chain.ainvoke(input_dict)

    def invoke(self, input_dict: dict) -> dict:
        """Sync invocation of the RAG pipeline."""
        import asyncio
        return asyncio.run(self.ainvoke(input_dict))

    @classmethod
    def _apply_search_kwargs(cls, retriever: Any, search_kwargs: Optional[SearchKwargs] = None, search_type: Optional[SearchType] = None) -> Any:
        """Apply search kwargs and search type to the retriever if they exist"""
        if hasattr(retriever, 'search_kwargs') and search_kwargs:
            # Convert search kwargs to dict, excluding None values
            kwargs_dict = search_kwargs.model_dump(exclude_none=True)

            # Only include relevant parameters based on search type
            if search_type == SearchType.SIMILARITY:
                # Remove MMR and similarity threshold specific params
                kwargs_dict.pop('fetch_k', None)
                kwargs_dict.pop('lambda_mult', None)
                kwargs_dict.pop('score_threshold', None)
            elif search_type == SearchType.MMR:
                # Remove similarity threshold specific params
                kwargs_dict.pop('score_threshold', None)
            elif search_type == SearchType.SIMILARITY_SCORE_THRESHOLD:
                # Remove MMR specific params
                kwargs_dict.pop('fetch_k', None)
                kwargs_dict.pop('lambda_mult', None)

            retriever.search_kwargs.update(kwargs_dict)

            # Set search type if supported by the retriever
            if hasattr(retriever, 'search_type') and search_type:
                retriever.search_type = search_type.value

        return retriever

    @classmethod
    def from_retriever(cls, config: RAGPipelineModel):
        """
        Builds a RAG pipeline with returned sources using a simple vector store retriever
        :param config: RAGPipelineModel
        :return:
        """
        vector_store_operator = VectorStoreOperator(
            vector_store=config.vector_store,
            documents=config.documents,
            embedding_model=config.embedding_model,
            vector_store_config=config.vector_store_config
        )
        retriever = vector_store_operator.vector_store.as_retriever()
        retriever = cls._apply_search_kwargs(retriever, config.search_kwargs, config.search_type)

        return cls(
            retriever,
            config.rag_prompt_template,
            config.llm,
            vector_store_config=config.vector_store_config,
            reranker=config.reranker,
            reranker_config=config.reranker_config,
            summarization_config=config.summarization_config
        )

    @classmethod
    def from_auto_retriever(cls, config: RAGPipelineModel):
        if not config.retriever_prompt_template:
            config.retriever_prompt_template = DEFAULT_AUTO_META_PROMPT_TEMPLATE

        retriever = AutoRetriever(config=config).as_runnable()
        retriever = cls._apply_search_kwargs(retriever, config.search_kwargs, config.search_type)
        return cls(
            retriever,
            config.rag_prompt_template,
            config.llm,
            reranker_config=config.reranker_config,
            reranker=config.reranker,
            vector_store_config=config.vector_store_config,
            summarization_config=config.summarization_config
        )

    @classmethod
    def from_multi_vector_retriever(cls, config: RAGPipelineModel):
        retriever = MultiVectorRetriever(config=config).as_runnable()
        retriever = cls._apply_search_kwargs(retriever, config.search_kwargs, config.search_type)
        return cls(
            retriever,
            config.rag_prompt_template,
            config.llm,
            reranker_config=config.reranker_config,
            reranker=config.reranker,
            vector_store_config=config.vector_store_config,
            summarization_config=config.summarization_config
        )

    @classmethod
    def from_sql_retriever(cls, config: RAGPipelineModel):
        retriever_config = config.sql_retriever_config
        if retriever_config is None:
            raise ValueError('Must provide "sql_retriever_config" for RAG pipeline config')
        vector_store_config = config.vector_store_config
        knowledge_base_table = vector_store_config.kb_table if vector_store_config is not None else None
        if knowledge_base_table is None:
            raise ValueError('Must provide valid "vector_store_config" for RAG pipeline config')
        embedding_args = knowledge_base_table._kb.embedding_model.learn_args.get('using', {})
        embeddings = construct_embedding_model_from_args(embedding_args)
        sql_llm = create_chat_model({
            'model_name': retriever_config.llm_config.model_name,
            'provider': retriever_config.llm_config.provider,
            **retriever_config.llm_config.params
        })
        vector_store_operator = VectorStoreOperator(
            vector_store=config.vector_store,
            documents=config.documents,
            embedding_model=config.embedding_model,
            vector_store_config=config.vector_store_config
        )
        vector_store_retriever = vector_store_operator.vector_store.as_retriever()
        vector_store_retriever = cls._apply_search_kwargs(vector_store_retriever, config.search_kwargs, config.search_type)
        distance_function = DistanceFunction.SQUARED_EUCLIDEAN_DISTANCE
        if config.vector_store_config.is_sparse and config.vector_store_config.vector_size is not None:
            # Use negative dot product for sparse retrieval.
            distance_function = DistanceFunction.NEGATIVE_DOT_PRODUCT
        retriever = SQLRetriever(
            fallback_retriever=vector_store_retriever,
            vector_store_handler=knowledge_base_table.get_vector_db(),
            min_k=retriever_config.min_k,
            max_filters=retriever_config.max_filters,
            filter_threshold=retriever_config.filter_threshold,
            database_schema=retriever_config.database_schema,
            embeddings_model=embeddings,
            search_kwargs=config.search_kwargs,
            rewrite_prompt_template=retriever_config.rewrite_prompt_template,
            table_prompt_template=retriever_config.table_prompt_template,
            column_prompt_template=retriever_config.column_prompt_template,
            value_prompt_template=retriever_config.value_prompt_template,
            boolean_system_prompt=retriever_config.boolean_system_prompt,
            generative_system_prompt=retriever_config.generative_system_prompt,
            num_retries=retriever_config.num_retries,
            embeddings_table=knowledge_base_table._kb.vector_database_table,
            source_table=retriever_config.source_table,
            source_id_column=retriever_config.source_id_column,
            distance_function=distance_function,
            llm=sql_llm
        )
        return cls(
            retriever,
            config.rag_prompt_template,
            config.llm,
            reranker_config=config.reranker_config,
            reranker=config.reranker,
            vector_store_config=config.vector_store_config,
            summarization_config=config.summarization_config
        )
