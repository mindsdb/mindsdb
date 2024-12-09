import pytest
from unittest.mock import patch, MagicMock
from mindsdb.interfaces.agents.langchain_agent import get_embedding_model_provider, build_embedding_model, create_chat_model


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


@patch('mindsdb.interfaces.agents.langchain_agent.get_chat_model_params')
def test_create_chat_model(mock_get_params):
    # Test OpenAI model creation
    args = {
        "provider": "openai",
        "model_name": "gpt-4",
        "api_key": "test-key",
        "base_url": "http://custom-endpoint/v1"
    }
    mock_get_params.return_value = {
        "model_name": "gpt-4",
        "openai_api_key": "test-key",
        "base_url": "http://custom-endpoint/v1"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatOpenAI') as mock_openai:
        mock_chat = MagicMock()
        mock_openai.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_openai.assert_called_once()
        call_args = mock_openai.call_args[1]
        assert call_args["model_name"] == "gpt-4"
        assert call_args["openai_api_key"] == "test-key"
        assert call_args["base_url"] == "http://custom-endpoint/v1"

    # Test Anthropic model creation
    args = {
        "provider": "anthropic",
        "model_name": "claude-3",
        "api_key": "test-key"
    }
    mock_get_params.return_value = {
        "model_name": "claude-3",
        "anthropic_api_key": "test-key"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatAnthropic') as mock_anthropic:
        mock_chat = MagicMock()
        mock_anthropic.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_anthropic.assert_called_once()
        call_args = mock_anthropic.call_args[1]
        assert call_args["model_name"] == "claude-3"
        assert call_args["anthropic_api_key"] == "test-key"

    # Test VLLM model creation
    args = {
        "provider": "vllm",
        "model_name": "llama2",
        "base_url": "http://vllm:8000/v1"
    }
    mock_get_params.return_value = {
        "model_name": "llama2",
        "base_url": "http://vllm:8000/v1"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatOpenAI') as mock_openai:
        mock_chat = MagicMock()
        mock_openai.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_openai.assert_called_once()
        call_args = mock_openai.call_args[1]
        assert call_args["model_name"] == "llama2"
        assert call_args["base_url"] == "http://vllm:8000/v1"

    # Test LiteLLM model creation
    args = {
        "provider": "litellm",
        "model_name": "gpt-4",
        "api_key": "test-key"
    }
    mock_get_params.return_value = {
        "model_name": "gpt-4",
        "api_key": "test-key"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatLiteLLM') as mock_litellm:
        mock_chat = MagicMock()
        mock_litellm.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_litellm.assert_called_once()
        call_args = mock_litellm.call_args[1]
        assert call_args["model_name"] == "gpt-4"
        assert call_args["api_key"] == "test-key"

    # Test Anyscale model creation
    args = {
        "provider": "anyscale",
        "model_name": "meta-llama/Llama-2-70b-chat-hf",
        "api_key": "test-key",
        "base_url": "http://anyscale:8000/v1"
    }
    mock_get_params.return_value = {
        "model_name": "meta-llama/Llama-2-70b-chat-hf",
        "anyscale_api_key": "test-key",
        "base_url": "http://anyscale:8000/v1"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatAnyscale') as mock_anyscale:
        mock_chat = MagicMock()
        mock_anyscale.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_anyscale.assert_called_once()
        call_args = mock_anyscale.call_args[1]
        assert call_args["model_name"] == "meta-llama/Llama-2-70b-chat-hf"
        assert call_args["anyscale_api_key"] == "test-key"
        assert call_args["base_url"] == "http://anyscale:8000/v1"

    # Test Ollama model creation
    args = {
        "provider": "ollama",
        "model_name": "llama2",
        "base_url": "http://ollama:11434"
    }
    mock_get_params.return_value = {
        "model": "llama2",
        "base_url": "http://ollama:11434"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatOllama') as mock_ollama:
        mock_chat = MagicMock()
        mock_ollama.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_ollama.assert_called_once()
        call_args = mock_ollama.call_args[1]
        assert call_args["model"] == "llama2"
        assert call_args["base_url"] == "http://ollama:11434"

    # Test NVIDIA NIM model creation
    args = {
        "provider": "nvidia_nim",
        "model_name": "mixtral-8x7b",
        "api_key": "test-key",
        "base_url": "http://nvidia:8000/v1"
    }
    mock_get_params.return_value = {
        "model_name": "mixtral-8x7b",
        "nvidia_api_key": "test-key",
        "base_url": "http://nvidia:8000/v1"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatNVIDIA') as mock_nvidia:
        mock_chat = MagicMock()
        mock_nvidia.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_nvidia.assert_called_once()
        call_args = mock_nvidia.call_args[1]
        assert call_args["model_name"] == "mixtral-8x7b"
        assert call_args["nvidia_api_key"] == "test-key"
        assert call_args["base_url"] == "http://nvidia:8000/v1"

    # Test MindsDB model creation
    args = {
        "provider": "mindsdb",
        "model_name": "my-model",
        "project_id": 1,
        "model_id": "abc123"
    }
    mock_get_params.return_value = {
        "model_name": "my-model",
        "project_id": 1,
        "model_id": "abc123"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatMindsdb') as mock_mindsdb:
        mock_chat = MagicMock()
        mock_mindsdb.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_mindsdb.assert_called_once()
        call_args = mock_mindsdb.call_args[1]
        assert call_args["model_name"] == "my-model"
        assert call_args["project_id"] == 1
        assert call_args["model_id"] == "abc123"

    # Test token counting fallback for OpenAI
    args = {
        "provider": "openai",
        "model_name": "gpt-4-new",  # A hypothetical new model without tiktoken support
        "api_key": "test-key"
    }
    mock_get_params.return_value = {
        "model_name": "gpt-4-new",
        "openai_api_key": "test-key"
    }
    with patch('mindsdb.interfaces.agents.langchain_agent.ChatOpenAI') as mock_openai:
        mock_chat = MagicMock()
        mock_chat.get_num_tokens_from_messages.side_effect = NotImplementedError()
        mock_openai.return_value = mock_chat
        model = create_chat_model(args)
        assert model == mock_chat
        mock_openai.assert_called_once()
        # Verify that tiktoken_model_name was set to default
        assert mock_chat.tiktoken_model_name == "gpt-4"

    # Test invalid provider
    args = {
        "provider": "invalid_provider",
        "model_name": "test-model"
    }
    with pytest.raises(ValueError) as exc_info:
        create_chat_model(args)
    assert "Unknown provider" in str(exc_info.value)
