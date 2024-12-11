from unittest.mock import Mock

from mindsdb.integrations.utilities.rag.settings import (
    RetrieverType, MultiVectorRetrieverMode, SearchType,
    RAGPipelineModel
)
from mindsdb.integrations.utilities.rag.config_loader import load_rag_config


def test_load_rag_config_empty():
    """Test loading RAG config with empty parameters"""
    config = load_rag_config({})
    assert isinstance(config, RAGPipelineModel)


def test_load_rag_config_basic():
    """Test loading RAG config with basic parameters"""
    base_config = {
        'retriever_type': RetrieverType.VECTOR_STORE.value,
        'search_type': SearchType.SIMILARITY.value
    }
    config = load_rag_config(base_config)

    assert isinstance(config, RAGPipelineModel)
    assert config.retriever_type == RetrieverType.VECTOR_STORE
    assert config.search_type == SearchType.SIMILARITY


def test_load_rag_config_with_search_kwargs():
    """Test loading RAG config with search kwargs"""
    base_config = {
        'retriever_type': RetrieverType.VECTOR_STORE.value,
        'search_type': SearchType.SIMILARITY.value,
        'search_kwargs': {'k': 5}
    }
    config = load_rag_config(base_config)

    assert isinstance(config, RAGPipelineModel)
    assert config.search_kwargs.k == 5


def test_load_rag_config_with_embedding_model():
    """Test loading RAG config with embedding model"""
    base_config = {
        'retriever_type': RetrieverType.VECTOR_STORE.value,
        'search_type': SearchType.SIMILARITY.value
    }
    embedding_model = Mock()
    config = load_rag_config(base_config, embedding_model=embedding_model)

    assert isinstance(config, RAGPipelineModel)
    assert config.embedding_model == embedding_model


def test_load_rag_config_with_multi_vector_mode():
    """Test loading RAG config with multi vector mode"""
    base_config = {
        'retriever_type': RetrieverType.VECTOR_STORE.value,
        'search_type': SearchType.SIMILARITY.value,
        'multi_vector_mode': MultiVectorRetrieverMode.CONCATENATE.value
    }
    config = load_rag_config(base_config)

    assert isinstance(config, RAGPipelineModel)
    assert config.multi_retriever_mode == MultiVectorRetrieverMode.CONCATENATE


def test_load_rag_config_with_kb_params():
    """Test loading RAG config with knowledge base parameters"""
    base_config = {
        'retriever_type': RetrieverType.VECTOR_STORE.value,
        'search_type': SearchType.SIMILARITY.value
    }
    kb_params = {
        'search_kwargs': {'k': 5}
    }
    config = load_rag_config(base_config, kb_params)

    assert isinstance(config, RAGPipelineModel)
    assert config.search_kwargs.k == 5


def test_load_rag_config_with_vector_store_config():
    """Test loading RAG config with vector store config"""
    base_config = {
        'retriever_type': RetrieverType.VECTOR_STORE.value,
        'search_type': SearchType.SIMILARITY.value
    }
    kb_params = {
        'vector_store_config': {'kb_table': Mock()}
    }
    config = load_rag_config(base_config, kb_params)

    assert isinstance(config, RAGPipelineModel)
    assert config.vector_store_config.kb_table == kb_params['vector_store_config']['kb_table']


def test_load_rag_config_from_knowledge_base():
    """Test RAG config loading in knowledge base context"""
    base_config = {
        'retriever_type': RetrieverType.VECTOR_STORE.value,
        'search_type': SearchType.SIMILARITY.value,
        'search_kwargs': {'k': 5}
    }
    kb_params = {
        'vector_store_config': {
            'kb_table': Mock()
        }
    }
    config = load_rag_config(base_config, kb_params)

    assert isinstance(config, RAGPipelineModel)
    assert config.retriever_type == RetrieverType.VECTOR_STORE
    assert config.search_type == SearchType.SIMILARITY
    assert config.search_kwargs.k == 5
    assert config.vector_store_config.kb_table == kb_params['vector_store_config']['kb_table']
