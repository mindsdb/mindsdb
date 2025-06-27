import pandas as pd
from langchain.storage import InMemoryByteStore
from langchain_core.runnables import RunnableSerializable
from mindsdb.integrations.utilities.rag.pipelines.rag import LangChainRAGPipeline
from mindsdb.integrations.utilities.rag.settings import (
    RetrieverType,
    RAGPipelineModel
)
from mindsdb.integrations.utilities.rag.utils import documents_to_df
from mindsdb.integrations.utilities.rag.retrievers.multi_hop_retriever import MultiHopRetriever
from mindsdb.utilities.log import getLogger
from langchain_text_splitters import RecursiveCharacterTextSplitter

logger = getLogger(__name__)

_retriever_strategies = {
    RetrieverType.VECTOR_STORE: lambda config: _create_pipeline_from_vector_store(config),
    RetrieverType.AUTO: lambda config: _create_pipeline_from_auto_retriever(config),
    RetrieverType.MULTI: lambda config: _create_pipeline_from_multi_retriever(config),
    RetrieverType.SQL: lambda config: _create_pipeline_from_sql_retriever(config),
    RetrieverType.MULTI_HOP: lambda config: _create_pipeline_from_multi_hop_retriever(config)
}


def _create_pipeline_from_vector_store(config: RAGPipelineModel) -> LangChainRAGPipeline:

    return LangChainRAGPipeline.from_retriever(
        config=config
    )


def _create_pipeline_from_auto_retriever(config: RAGPipelineModel) -> LangChainRAGPipeline:
    return LangChainRAGPipeline.from_auto_retriever(
        config=config
    )


def _create_pipeline_from_multi_retriever(config: RAGPipelineModel) -> LangChainRAGPipeline:

    if config.text_splitter is None:
        config.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.chunk_size, chunk_overlap=config.chunk_overlap
        )
    if config.parent_store is None:
        config.parent_store = InMemoryByteStore()

    return LangChainRAGPipeline.from_multi_vector_retriever(
        config=config
    )


def _create_pipeline_from_sql_retriever(config: RAGPipelineModel) -> LangChainRAGPipeline:
    return LangChainRAGPipeline.from_sql_retriever(
        config=config
    )


def _create_pipeline_from_multi_hop_retriever(config: RAGPipelineModel) -> LangChainRAGPipeline:
    retriever = MultiHopRetriever.from_config(config)
    return LangChainRAGPipeline(
        retriever_runnable=retriever,
        prompt_template=config.rag_prompt_template,
        llm=config.llm,
        reranker_config=config.reranker_config,
        reranker=config.reranker,
        vector_store_config=config.vector_store_config,
        summarization_config=config.summarization_config
    )


def _process_documents_to_df(config: RAGPipelineModel) -> pd.DataFrame:
    return documents_to_df(config.content_column_name,
                           config.documents,
                           embedding_model=config.embedding_model,
                           with_embeddings=True)


def get_pipeline_from_retriever(config: RAGPipelineModel) -> RunnableSerializable:
    retriever_strategy = _retriever_strategies.get(config.retriever_type)
    if retriever_strategy:
        return retriever_strategy(config).with_returned_sources()
    else:
        raise ValueError(
            f'Invalid retriever type, must be one of: {list(_retriever_strategies.keys())}. Got {config.retriever_type}')


class RAG:
    def __init__(self, config: RAGPipelineModel):
        self.pipeline = get_pipeline_from_retriever(config)

    def __call__(self, question: str) -> dict:
        logger.info(f"Processing question using rag pipeline: {question}")
        result = self.pipeline.invoke(question)

        returned_sources = [docs.page_content for docs in result['context']]
        logger.info(f"retrieved context used to answer question: {returned_sources}")

        return result
