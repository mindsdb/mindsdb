import pandas as pd

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.runnables import RunnableSerializable
from sqlalchemy import create_engine
from mindsdb.integrations.utilities.rag.pipelines.rag import LangChainRAGPipeline
from mindsdb.integrations.utilities.rag.settings import (
    DEFAULT_CHUNK_OVERLAP,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_EVALUATION_PROMPT_TEMPLATE,
    DEFAULT_POOL_RECYCLE,
    RetrieverType,
    RAGPipelineModel
)
from mindsdb.integrations.utilities.rag.utils import documents_to_df, VectorStoreOperator


_retriever_strategies = {
    RetrieverType.SQL: lambda config: create_pipeline_from_sql_retriever(config),
    RetrieverType.VECTOR_STORE: lambda config: create_pipeline_from_vector_store(config),
    RetrieverType.AUTO: lambda config: create_pipeline_from_auto_retriever(config),
    RetrieverType.MULTI: lambda config: create_pipeline_from_multi_retriever(config),
}


def create_pipeline_from_sql_retriever(config: RAGPipelineModel) -> LangChainRAGPipeline:
    documents_df = process_documents_to_df(config)
    alchemyEngine = create_engine(
        config.db_connection_string, pool_recycle=DEFAULT_POOL_RECYCLE)
    db_connection = alchemyEngine.connect()

    documents_df.to_sql(config.test_table_name, db_connection, index=False, if_exists='replace')

    return LangChainRAGPipeline.from_sql_retriever(
        connection_string=config.db_connection_string,
        retriever_prompt_template=config.retriever_prompt_template,
        rag_prompt_template=config.rag_prompt_template,
        llm=config.llm
    )


def create_pipeline_from_vector_store(config: RAGPipelineModel) -> LangChainRAGPipeline:
    vector_store_operator = VectorStoreOperator(
        vector_store=config.vector_store,
        documents=config.documents,
        embeddings_model=config.embeddings_model
    )

    return LangChainRAGPipeline.from_retriever(
        retriever=vector_store_operator.vector_store.as_retriever(),
        prompt_template=DEFAULT_EVALUATION_PROMPT_TEMPLATE,
        llm=config.llm
    )


def create_pipeline_from_auto_retriever(config: RAGPipelineModel) -> LangChainRAGPipeline:
    return LangChainRAGPipeline.from_auto_retriever(
        vectorstore=config.vector_store,
        data=config.documents,
        data_description=config.dataset_description,
        content_column_name=config.content_column_name,
        retriever_prompt_template=config.retriever_prompt_template,
        rag_prompt_template=DEFAULT_EVALUATION_PROMPT_TEMPLATE,
        llm=config.llm
    )


def create_pipeline_from_multi_retriever(config: RAGPipelineModel) -> LangChainRAGPipeline:
    child_text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=DEFAULT_CHUNK_SIZE, chunk_overlap=DEFAULT_CHUNK_OVERLAP)

    return LangChainRAGPipeline.from_multi_vector_retriever(
        documents=config.documents,
        rag_prompt_template=DEFAULT_EVALUATION_PROMPT_TEMPLATE,
        vectorstore=config.vector_store,
        text_splitter=child_text_splitter,
        llm=config.llm,
        mode=config.multi_retriever_mode
    )


def process_documents_to_df(config: RAGPipelineModel) -> pd.DataFrame:
    return documents_to_df(config.content_column_name,
                           config.documents,
                           embeddings_model=config.embeddings_model,
                           with_embeddings=True)


def get_pipeline_from_retriever(config: RAGPipelineModel) -> RunnableSerializable:
    retriever_strategy = _retriever_strategies.get(config.retriever_type)
    if retriever_strategy:
        return retriever_strategy(config).with_returned_sources()
    else:
        raise ValueError(
            f'Invalid retriever type, must be one of: {list(_retriever_strategies.keys())}. Got {config.retriever_type}')
