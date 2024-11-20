import pytest
from unittest.mock import Mock
from mindsdb.integrations.utilities.rag.settings import (
    RAGPipelineModel,
    RetrieverType,
    MultiVectorRetrieverMode,
    VectorStoreConfig,
    DEFAULT_LLM_MODEL, DEFAULT_TEST_TABLE_NAME, DEFAULT_CHUNK_SIZE
)
from mindsdb.interfaces.skills.retrieval_tool import _get_rag_params


@pytest.fixture
def mock_tools_config():
    return {
        'retriever_type': 'vector_store',
        'multi_retriever_mode': 'both',
        'embedding_model': Mock(),
        'documents': [Mock()],
        'vector_store_config': {
            'vector_store_type': 'chromadb',
            'collection_name': 'test'
        },
        'invalid_param': 'should_be_filtered_out'
    }


def test_rag_params_conversion():
    """Test that parameters are correctly converted to RAGPipelineModel"""
    tools_config = {
        'retriever_type': 'vector_store',
        'multi_retriever_mode': 'both',
    }
    rag_params = _get_rag_params(tools_config)
    rag_config = RAGPipelineModel(**rag_params)
    assert rag_config.retriever_type == RetrieverType.VECTOR_STORE
    assert rag_config.multi_retriever_mode == MultiVectorRetrieverMode.BOTH


def test_invalid_enum_values():
    """Test that invalid enum values raise appropriate errors"""
    tools_config = {
        'retriever_type': 'invalid_type',
    }
    with pytest.raises(ValueError):
        rag_params = _get_rag_params(tools_config)
        RAGPipelineModel(**rag_params)


def test_vector_store_config_conversion():
    """Test that vector store config is properly handled"""
    tools_config = {
        'vector_store_config': {
            'vector_store_type': 'chromadb',
            'collection_name': 'test'
        }
    }
    rag_params = _get_rag_params(tools_config)
    rag_config = RAGPipelineModel(**rag_params)
    assert isinstance(rag_config.vector_store_config, VectorStoreConfig)
    assert rag_config.vector_store_config.collection_name == 'test'


def test_default_values():
    """Test that default values are properly set"""
    tools_config = {}
    rag_params = _get_rag_params(tools_config)
    rag_config = RAGPipelineModel(**rag_params)
    # Test default enum values
    assert rag_config.retriever_type == RetrieverType.VECTOR_STORE
    assert rag_config.multi_retriever_mode == MultiVectorRetrieverMode.BOTH
    # Test other default values
    assert rag_config.llm_model_name == DEFAULT_LLM_MODEL
    assert rag_config.table_name == DEFAULT_TEST_TABLE_NAME
    assert rag_config.chunk_size == DEFAULT_CHUNK_SIZE
    assert isinstance(rag_config.vector_store_config, VectorStoreConfig)


@pytest.mark.parametrize("field,value,expected", [
    ('retriever_type', 'auto', RetrieverType.AUTO),
    ('multi_retriever_mode', 'split', MultiVectorRetrieverMode.SPLIT),
    ('chunk_size', 500, 500),
])
def test_field_assignments(field, value, expected):
    """Test various field assignments"""
    tools_config = {field: value}
    rag_params = _get_rag_params(tools_config)
    rag_config = RAGPipelineModel(**rag_params)
    assert getattr(rag_config, field) == expected


def test_filtering_invalid_params():
    """Test that invalid parameters are filtered out"""
    tools_config = {
        'invalid_param': 'should_be_filtered',
        'retriever_type': 'vector_store'
    }
    rag_params = _get_rag_params(tools_config)
    assert 'invalid_param' not in rag_params
    RAGPipelineModel(**rag_params)  # Should not raise any errors
