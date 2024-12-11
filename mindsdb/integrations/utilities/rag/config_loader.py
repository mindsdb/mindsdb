"""Utility functions for RAG pipeline configuration"""
from typing import Dict, Any, Optional

from mindsdb.utilities.log import getLogger
from mindsdb.integrations.utilities.rag.settings import (
    RetrieverType, MultiVectorRetrieverMode, SearchType,
    SearchKwargs, SummarizationConfig, VectorStoreConfig,
    RerankerConfig, RAGPipelineModel, DEFAULT_COLLECTION_NAME
)

logger = getLogger(__name__)


def load_rag_config(base_config: Dict[str, Any], kb_params: Optional[Dict[str, Any]] = None, embedding_model: Any = None) -> RAGPipelineModel:
    """
    Load and validate RAG configuration parameters. This function handles the conversion of configuration
    parameters into their appropriate types and ensures all required settings are properly configured.

    Args:
        base_config: Base configuration dictionary containing RAG pipeline settings
        kb_params: Optional knowledge base parameters to merge with base config
        embedding_model: Optional embedding model instance to use in the RAG pipeline

    Returns:
        RAGPipelineModel: Validated RAG configuration model ready for pipeline creation

    Raises:
        ValueError: If configuration validation fails or required parameters are missing
    """
    # Create a shallow copy of the base config to avoid modifying the original
    # We avoid deepcopy because some objects (like embedding_model) may contain unpickleable objects
    rag_params = base_config.copy()

    # Merge with knowledge base params if provided
    if kb_params:
        rag_params.update(kb_params)

    # Set embedding model if provided
    if embedding_model is not None:
        rag_params['embedding_model'] = embedding_model

    # Handle enums and type conversions
    if 'retriever_type' in rag_params:
        rag_params['retriever_type'] = RetrieverType(rag_params['retriever_type'])
    if 'multi_retriever_mode' in rag_params:
        rag_params['multi_retriever_mode'] = MultiVectorRetrieverMode(rag_params['multi_retriever_mode'])
    if 'search_type' in rag_params:
        rag_params['search_type'] = SearchType(rag_params['search_type'])

    # Handle search kwargs if present
    if 'search_kwargs' in rag_params and isinstance(rag_params['search_kwargs'], dict):
        rag_params['search_kwargs'] = SearchKwargs(**rag_params['search_kwargs'])

    # Handle summarization config if present
    summarization_config = rag_params.get('summarization_config')
    if summarization_config is not None and isinstance(summarization_config, dict):
        rag_params['summarization_config'] = SummarizationConfig(**summarization_config)

    # Handle vector store config
    if 'vector_store_config' in rag_params:
        if isinstance(rag_params['vector_store_config'], dict):
            rag_params['vector_store_config'] = VectorStoreConfig(**rag_params['vector_store_config'])
    else:
        rag_params['vector_store_config'] = {}
        logger.warning(f'No collection_name specified for the retrieval tool, '
                       f"using default collection_name: '{DEFAULT_COLLECTION_NAME}'"
                       f'\nWarning: If this collection does not exist, no data will be retrieved')

    if 'reranker_config' in rag_params:
        rag_params['reranker_config'] = RerankerConfig(**rag_params['reranker_config'])

    # Convert to RAGPipelineModel with validation
    try:
        return RAGPipelineModel(**rag_params)
    except Exception as e:
        logger.error(f"Invalid RAG configuration: {str(e)}")
        raise ValueError(f"Configuration validation failed: {str(e)}")
