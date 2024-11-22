from copy import copy
from typing import Optional, Any

from langchain_core.output_parsers import StrOutputParser
from langchain.retrievers import ContextualCompressionRetriever

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableSerializable

from mindsdb.integrations.utilities.rag.retrievers.auto_retriever import AutoRetriever
from mindsdb.integrations.utilities.rag.retrievers.multi_vector_retriever import MultiVectorRetriever
from mindsdb.integrations.utilities.rag.rerankers.reranker_compressor import LLMReranker, RerankerConfig
from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel, DEFAULT_AUTO_META_PROMPT_TEMPLATE, SearchKwargs, SearchType
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RERANKER_FLAG

from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator


class LangChainRAGPipeline:
    """
    Builds a RAG pipeline using langchain LCEL components
    """

    def __init__(self, retriever_runnable, prompt_template, llm, reranker: bool = DEFAULT_RERANKER_FLAG,
                 reranker_config: Optional[RerankerConfig] = None):

        self.retriever_runnable = retriever_runnable
        self.prompt_template = prompt_template
        self.llm = llm
        if reranker:
            if reranker_config is None:
                reranker_config = RerankerConfig()
            self.reranker = LLMReranker(model=reranker_config.model, base_url=reranker_config.base_url,
                                        filtering_threshold=reranker_config.filtering_threshold)
        else:
            self.reranker = None

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
            return "\n\n".join(doc.page_content for doc in docs)

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

        rag_chain_with_source = RunnableParallel(
            {"context": self.retriever_runnable, "question": RunnablePassthrough()}
        ).assign(answer=rag_chain_from_docs)

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
        return cls(retriever, config.rag_prompt_template, config.llm)

    @classmethod
    def from_auto_retriever(cls, config: RAGPipelineModel):
        if not config.retriever_prompt_template:
            config.retriever_prompt_template = DEFAULT_AUTO_META_PROMPT_TEMPLATE

        retriever = AutoRetriever(config=config).as_runnable()
        retriever = cls._apply_search_kwargs(retriever, config.search_kwargs, config.search_type)
        return cls(retriever, config.rag_prompt_template, config.llm)

    @classmethod
    def from_multi_vector_retriever(cls, config: RAGPipelineModel):
        retriever = MultiVectorRetriever(config=config).as_runnable()
        retriever = cls._apply_search_kwargs(retriever, config.search_kwargs, config.search_type)
        return cls(retriever, config.rag_prompt_template, config.llm)
