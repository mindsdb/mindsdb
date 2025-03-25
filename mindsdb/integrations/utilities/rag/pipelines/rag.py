from copy import copy
from typing import Optional, Any, List

from langchain_core.output_parsers import StrOutputParser
from langchain.retrievers import ContextualCompressionRetriever
from langchain_core.documents import Document

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableSerializable

from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
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
from mindsdb.interfaces.agents.langchain_agent import create_chat_model


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

    def with_returned_sources(self) -> RunnableSerializable:
        """
        Builds a RAG pipeline with returned sources
        :return:
        """

        def format_docs(docs):
            if isinstance(docs, str):
                # this is to handle the case where the retriever returns a string
                # instead of a list of documents e.g. SQLRetriever
                return docs
            if not docs:
                return ''
            # Sort by original document so we can group source summaries together.
            docs.sort(key=lambda d: d.metadata.get('original_row_id') if d.metadata else 0)
            original_document_id = None
            summary_prepended_text = 'Summary of the original document that the below context was taken from:\n'
            document_content = ''
            for d in docs:
                metadata = d.metadata or {}
                if metadata.get('original_row_id') != original_document_id and metadata.get('summary'):
                    # We have a summary of a new document to prepend.
                    original_document_id = metadata.get('original_row_id')
                    summary = f"{summary_prepended_text}{metadata.get('summary')}\n"
                    document_content += summary
                document_content += f'{d.page_content}\n\n'
            return document_content

        prompt = ChatPromptTemplate.from_template(self.prompt_template)

        # Ensure all the required components are not None
        if prompt is None:
            raise ValueError("One of the required components (prompt) is None")
        if self.llm is None:
            raise ValueError("One of the required components (llm) is None")

        if self.reranker:
            # Create a custom retriever that handles async operations properly
            class AsyncRerankerRetriever(ContextualCompressionRetriever):
                """Async-aware retriever that properly handles concurrent reranking operations."""

                def __init__(self, base_retriever, reranker):
                    super().__init__(
                        base_compressor=reranker,
                        base_retriever=base_retriever
                    )

                async def ainvoke(self, query: str) -> List[Document]:
                    """Async retrieval with proper concurrency handling."""
                    # Get initial documents
                    if hasattr(self.base_retriever, 'ainvoke'):
                        docs = await self.base_retriever.ainvoke(query)
                    else:
                        docs = await RunnablePassthrough(self.base_retriever.get_relevant_documents)(query)

                    # Rerank documents
                    if docs:
                        docs = await self.base_compressor.acompress_documents(docs, query)
                    return docs

                def get_relevant_documents(self, query: str) -> List[Document]:
                    """Sync wrapper for async retrieval."""
                    import asyncio
                    return asyncio.run(self.ainvoke(query))

            # Use our custom async-aware retriever
            self.retriever_runnable = AsyncRerankerRetriever(
                base_retriever=copy(self.retriever_runnable),
                reranker=self.reranker
            )

        rag_chain_from_docs = (
            RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))
            | prompt
            | self.llm
            | StrOutputParser()
        )

        retrieval_chain = RunnableParallel(
            context=self.retriever_runnable,
            question=RunnablePassthrough()
        )
        if self.summarizer is not None:
            retrieval_chain = retrieval_chain | self.summarizer

        rag_chain_with_source = retrieval_chain.assign(answer=rag_chain_from_docs)
        return rag_chain_with_source

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
        embeddings = construct_model_from_args(embedding_args)
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
