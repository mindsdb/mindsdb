from copy import copy
from typing import Optional, Any

from langchain_core.output_parsers import StrOutputParser
from langchain.retrievers import ContextualCompressionRetriever

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
            self.reranker = LLMReranker(
                model=reranker_config.model,
                base_url=reranker_config.base_url,
                filtering_threshold=reranker_config.filtering_threshold,
                num_docs_to_keep=reranker_config.num_docs_to_keep
            )
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
            reranker = self.reranker
            retriever = copy(self.retriever_runnable)
            self.retriever_runnable = ContextualCompressionRetriever(
                base_compressor=reranker, base_retriever=retriever
            )

        rag_chain_from_docs = (
                RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))  # noqa: E126, E122
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
        retriever = SQLRetriever(
            fallback_retriever=vector_store_retriever,
            vector_store_handler=knowledge_base_table.get_vector_db(),
            metadata_schemas=retriever_config.metadata_schemas,
            examples=retriever_config.examples,
            embeddings_model=embeddings,
            rewrite_prompt_template=retriever_config.rewrite_prompt_template,
            retry_prompt_template=retriever_config.query_retry_template,
            num_retries=retriever_config.num_retries,
            sql_prompt_template=retriever_config.sql_prompt_template,
            query_checker_template=retriever_config.query_checker_template,
            embeddings_table=knowledge_base_table._kb.vector_database_table,
            source_table=retriever_config.source_table,
            # Currently only similarity search is supported.
            distance_function=DistanceFunction.SQUARED_EUCLIDEAN_DISTANCE,
            search_kwargs=config.search_kwargs,
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
