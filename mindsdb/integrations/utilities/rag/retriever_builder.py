
from langchain.storage import InMemoryByteStore
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.retrievers import BaseRetriever

from mindsdb.integrations.utilities.rag.retrievers.auto_retriever import AutoRetriever
from mindsdb.integrations.utilities.rag.retrievers.multi_vector_retriever import MultiVectorRetriever
from mindsdb.integrations.utilities.rag.retrievers.sql_retriever import SQLRetriever
from mindsdb.integrations.utilities.rag.settings import (
    RetrieverType,
    RAGPipelineModel, DEFAULT_SQL_RETRIEVAL_PROMPT_TEMPLATE, DEFAULT_AUTO_META_PROMPT_TEMPLATE
)
from mindsdb.integrations.utilities.rag.utils import VectorStoreOperator

_retriever_strategies = {
    RetrieverType.SQL: lambda config: create_sql_retriever(config),
    RetrieverType.VECTOR_STORE: lambda config: create_vector_store_retriever(config),
    RetrieverType.AUTO: lambda config: create_auto_retriever(config),
    RetrieverType.MULTI: lambda config: create_multi_retriever(config),
}


def create_sql_retriever(config: RAGPipelineModel) -> BaseRetriever:
    # todo need to fix this one, not stable
    if not config.retriever_prompt_template:
        config.retriever_prompt_template = DEFAULT_SQL_RETRIEVAL_PROMPT_TEMPLATE

    return SQLRetriever(config=config).as_runnable()


def create_vector_store_retriever(config: RAGPipelineModel) -> BaseRetriever:

    vector_store_operator = VectorStoreOperator(
        vector_store=config.vector_store,
        documents=config.documents,
        embeddings_model=config.embeddings_model
    )

    return vector_store_operator.vector_store.as_retriever()


def create_auto_retriever(config: RAGPipelineModel) -> BaseRetriever:
    # todo need to fix to work with agent
    if not config.retriever_prompt_template:
        config.retriever_prompt_template = DEFAULT_AUTO_META_PROMPT_TEMPLATE

    return AutoRetriever(config=config).as_runnable()


def create_multi_retriever(config: RAGPipelineModel) -> BaseRetriever:

    if config.text_splitter is None:
        config.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.chunk_size, chunk_overlap=config.chunk_overlap
        )
    if config.parent_store is None:
        config.parent_store = InMemoryByteStore()

    return MultiVectorRetriever(config=config).as_runnable()


def build_retriever(config: RAGPipelineModel) -> BaseRetriever:
    retriever_strategy = _retriever_strategies.get(config.retriever_type)
    if retriever_strategy:
        return retriever_strategy(config)
    else:
        raise ValueError(
            f'Invalid retriever type, must be one of: {list(_retriever_strategies.keys())}. Got {config.retriever_type}')
