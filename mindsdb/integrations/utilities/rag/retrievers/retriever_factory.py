"""Factory functions for creating retrievers."""

from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel, RetrieverType
from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator
from mindsdb.integrations.utilities.rag.retrievers.auto_retriever import AutoRetriever
from mindsdb.integrations.utilities.rag.retrievers.sql_retriever import SQLRetriever


def create_vector_store_retriever(config: RAGPipelineModel):
    """Create a vector store retriever."""
    vector_store_operator = VectorStoreOperator(
        vector_store=config.vector_store,
        documents=config.documents,
        embedding_model=config.embedding_model,
        vector_store_config=config.vector_store_config
    )
    return vector_store_operator.vector_store.as_retriever()


def create_auto_retriever(config: RAGPipelineModel):
    """Create an auto retriever."""
    return AutoRetriever(
        vector_store=config.vector_store,
        documents=config.documents,
        embedding_model=config.embedding_model
    )


def create_sql_retriever(config: RAGPipelineModel):
    """Create a SQL retriever."""
    return SQLRetriever(
        sql_source=config.sql_source,
        llm=config.llm
    )


def create_retriever(config: RAGPipelineModel, retriever_type: RetrieverType = None):
    """Create a retriever based on type."""
    retriever_type = retriever_type or config.retriever_type

    if retriever_type == RetrieverType.VECTOR_STORE:
        return create_vector_store_retriever(config)
    elif retriever_type == RetrieverType.AUTO:
        return create_auto_retriever(config)
    elif retriever_type == RetrieverType.SQL:
        return create_sql_retriever(config)
    else:
        raise ValueError(f"Unsupported retriever type: {retriever_type}")
