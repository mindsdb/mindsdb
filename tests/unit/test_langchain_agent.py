import pytest
from unittest.mock import patch, MagicMock
from mindsdb.interfaces.agents.langchain_agent import get_embedding_model_provider, build_embedding_model


def test_vllm_embedding_configuration():
    # Test explicit VLLM configuration
    args = {
        'embedding_model_provider': 'vllm',
        'embedding_base_url': 'http://vllm_embeddings:8001/v1',
        'embedding_model_name': 'test-model'
    }

    # Should return 'openai' since VLLM uses OpenAI interface
    assert get_embedding_model_provider(args) == 'openai'

    # Test implicit VLLM configuration through provider
    args = {
        'provider': 'vllm',
        'base_url': 'http://vllm_llama3:8000/v1',
        'embedding_base_url': 'http://vllm_embeddings:8001/v1',
        'model_name': 'llm-model',
        'embedding_model_name': 'embedding-model'
    }
    assert get_embedding_model_provider(args) == 'openai'


def test_vllm_embedding_validation():
    # Test missing embedding_base_url with explicit config
    with pytest.raises(ValueError) as exc_info:
        get_embedding_model_provider({
            'embedding_model_provider': 'vllm'
        })
    assert 'embedding_base_url' in str(exc_info.value)

    # Test missing embedding_base_url with implicit config
    with pytest.raises(ValueError) as exc_info:
        get_embedding_model_provider({
            'provider': 'vllm',
            'base_url': 'http://vllm_llama3:8000/v1'
        })
    assert 'embedding_base_url' in str(exc_info.value)


@patch('mindsdb.interfaces.agents.langchain_agent.construct_model_from_args')
def test_build_embedding_model_vllm(mock_construct):
    mock_model = MagicMock()
    mock_construct.return_value = mock_model

    # Test explicit VLLM configuration
    args = {
        'embedding_model_provider': 'vllm',
        'embedding_base_url': 'http://vllm_embeddings:8001/v1',
        'embedding_model_name': 'test-model'
    }

    model = build_embedding_model(args.copy())
    assert model == mock_model

    # Verify OpenAI interface configuration
    call_args = mock_construct.call_args[0][0]
    assert call_args['class'] == 'openai'
    assert call_args['openai_api_base'] == 'http://vllm_embeddings:8001/v1'
    assert call_args['model'] == 'test-model'
    assert 'openai_api_key' in call_args

    # Test model name fallback
    args = {
        'provider': 'vllm',
        'embedding_base_url': 'http://vllm_embeddings:8001/v1',
        'model_name': 'fallback-model'
    }

    model = build_embedding_model(args.copy())
    call_args = mock_construct.call_args[0][0]
    assert call_args['model'] == 'fallback-model'  # Should fall back to model_name

    # Test default model name
    args = {
        'provider': 'vllm',
        'embedding_base_url': 'http://vllm_embeddings:8001/v1'
    }

    model = build_embedding_model(args.copy())
    call_args = mock_construct.call_args[0][0]
    assert call_args['model'] == 'text-embedding-ada-002'  # Should use default
