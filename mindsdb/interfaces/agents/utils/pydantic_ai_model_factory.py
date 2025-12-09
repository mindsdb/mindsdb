"""Pydantic AI model factory to create models from MindsDB configuration"""

from typing import Dict, Any, Optional
from mindsdb.utilities import log
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.libs.llm.utils import get_llm_config
from mindsdb.interfaces.agents.utils.constants import (
    OPEN_AI_CHAT_MODELS,
    ANTHROPIC_CHAT_MODELS,
    GOOGLE_GEMINI_CHAT_MODELS,
    OLLAMA_CHAT_MODELS,
    NVIDIA_NIM_CHAT_MODELS,
    WRITER_CHAT_MODELS,
    SUPPORTED_PROVIDERS,
    DEFAULT_EMBEDDINGS_MODEL_PROVIDER,
)

logger = log.getLogger(__name__)


def get_llm_provider(args: Dict) -> str:
    """
    Get LLM provider from args.
    
    Args:
        args: Dictionary containing model_name and optionally provider
        
    Returns:
        Provider name string
    """
    # If provider is explicitly specified, use that
    if "provider" in args:
        return args["provider"]

    # Check for known model names from other providers first
    model_name = args.get("model_name")
    if not model_name:
        raise ValueError("model_name is required to determine provider")
    
    if model_name in ANTHROPIC_CHAT_MODELS:
        return "anthropic"
    if model_name in OPEN_AI_CHAT_MODELS:
        return "openai"
    if model_name in OLLAMA_CHAT_MODELS:
        return "ollama"
    if model_name in NVIDIA_NIM_CHAT_MODELS:
        return "nvidia_nim"
    if model_name in GOOGLE_GEMINI_CHAT_MODELS:
        return "google"
    # Check for writer models
    if model_name in WRITER_CHAT_MODELS:
        return "writer"

    # For vLLM, require explicit provider specification
    raise ValueError("Invalid model name. Please define a supported llm provider")


def get_embedding_model_provider(args: Dict) -> str:
    """
    Get the embedding model provider from args.
    
    Args:
        args: Dictionary containing embedding model configuration
        
    Returns:
        Provider name string
    """
    # Check for explicit embedding model provider
    if "embedding_model_provider" in args:
        provider = args["embedding_model_provider"]
        if provider == "vllm":
            if not (args.get("openai_api_base") and args.get("model")):
                raise ValueError(
                    "VLLM embeddings configuration error:\n"
                    "- Missing required parameters: 'openai_api_base' and/or 'model'\n"
                    "- Example: openai_api_base='http://localhost:8003/v1', model='your-model-name'"
                )
            logger.info("Using custom VLLMEmbeddings class")
            return "vllm"
        return provider

    # Check if LLM provider is vLLM
    llm_provider = args.get("provider", DEFAULT_EMBEDDINGS_MODEL_PROVIDER)
    if llm_provider == "vllm":
        if not (args.get("openai_api_base") and args.get("model")):
            raise ValueError(
                "VLLM embeddings configuration error:\n"
                "- Missing required parameters: 'openai_api_base' and/or 'model'\n"
                "- When using VLLM as LLM provider, you must specify the embeddings server location and model\n"
                "- Example: openai_api_base='http://localhost:8003/v1', model='your-model-name'"
            )
        logger.info("Using custom VLLMEmbeddings class")
        return "vllm"

    # Default to LLM provider
    return llm_provider


def get_chat_model_params(args: Dict) -> Dict:
    """
    Get chat model parameters from args.
    
    Args:
        args: Dictionary containing model configuration
        
    Returns:
        Dictionary of formatted model parameters
    """
    model_config = args.copy()
    # Include API keys.
    model_config["api_keys"] = {p: get_api_key(p, model_config, None, strict=False) for p in SUPPORTED_PROVIDERS}
    llm_config = get_llm_config(args.get("provider", get_llm_provider(args)), model_config)
    config_dict = llm_config.model_dump(by_alias=True)
    config_dict = {k: v for k, v in config_dict.items() if v is not None}

    # If provider is writer, ensure the API key is passed as 'api_key'
    if args.get("provider") == "writer" and "writer_api_key" in config_dict:
        config_dict["api_key"] = config_dict.pop("writer_api_key")

    return config_dict


def get_pydantic_ai_model_kwargs(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get keyword arguments for Pydantic AI model initialization.
    
    This extracts API keys and other configuration needed by Pydantic AI models.
    API keys are retrieved from system configuration if not provided in args,
    following the same pattern as knowledge bases.
    
    Args:
        args: Dictionary containing model configuration
        
    Returns:
        Dictionary of keyword arguments for Pydantic AI model
    """
    from mindsdb.integrations.utilities.handler_utils import get_api_key
    
    kwargs = {}
    
    # Get provider
    provider = args.get("provider")
    if provider is None:
        provider = get_llm_provider(args)
    
    # Extract API keys based on provider, using get_api_key to get from system config if needed
    if provider == "openai" or provider == "vllm":
        # Try to get API key from args first, then from system config
        api_key = args.get("openai_api_key") or args.get("api_key")
        if not api_key:
            api_key = get_api_key("openai", args, strict=False)
        if api_key:
            kwargs["api_key"] = api_key
        
        base_url = args.get("openai_api_base") or args.get("base_url")
        if base_url:
            kwargs["base_url"] = base_url
        organization = args.get("openai_api_organization") or args.get("api_organization")
        if organization:
            kwargs["organization"] = organization
    
    elif provider == "anthropic":
        # Try to get API key from args first, then from system config
        api_key = args.get("anthropic_api_key") or args.get("api_key")
        if not api_key:
            api_key = get_api_key("anthropic", args, strict=False)
        if api_key:
            kwargs["api_key"] = api_key
        
        base_url = args.get("anthropic_api_url") or args.get("base_url")
        if base_url:
            kwargs["base_url"] = base_url
    
    elif provider == "google":
        # Try to get API key from args first, then from system config
        api_key = args.get("google_api_key") or args.get("api_key")
        if not api_key:
            api_key = get_api_key("google", args, strict=False)
        if api_key:
            kwargs["api_key"] = api_key
    
    elif provider == "ollama":
        base_url = args.get("ollama_base_url") or args.get("base_url")
        if base_url:
            kwargs["base_url"] = base_url
    
    # Add other common parameters
    if "temperature" in args:
        kwargs["temperature"] = args["temperature"]
    if "max_tokens" in args:
        kwargs["max_tokens"] = args["max_tokens"]
    if "top_p" in args:
        kwargs["top_p"] = args["top_p"]
    if "top_k" in args:
        kwargs["top_k"] = args["top_k"]
    
    return kwargs


def get_model_instance_from_kwargs(args: Dict[str, Any]) -> Any:
    """
    Create and return a Pydantic AI model instance from MindsDB args.
    
    This method creates an actual model instance that can be passed directly to
    Agent(model_instance) instead of using a model string.
    
    Args:
        args: Dictionary containing model configuration (provider, model_name, api keys, etc.)
               Similar format to what get_pydantic_ai_model_kwargs expects
        
    Returns:
        Model instance that can be passed to Pydantic AI Agent
        
    Raises:
        ValueError: If provider is not supported or model name is invalid
        ImportError: If required Pydantic AI model classes are not available
    """
    # Determine provider
    provider = args.get("provider")
    if provider is None:
        provider = get_llm_provider(args)
    
    model_name = args.get("model_name")
    if not model_name:
        raise ValueError("model_name is required")
    
    # Get model kwargs (includes API keys from system config)
    model_kwargs = get_pydantic_ai_model_kwargs(args)
    
    try:
        if provider == "openai" or provider == "vllm":
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider
            
            # Extract API key and other settings
            api_key = model_kwargs.pop("api_key", None)
            base_url = model_kwargs.pop("base_url", None)
            organization = model_kwargs.pop("organization", None)
            
            # For vLLM, ensure base_url is set if provided
            if provider == "vllm" and not base_url:
                base_url = args.get("openai_api_base") or args.get("base_url")
            
            # Create provider with API key and settings
            provider_kwargs = {}
            if api_key:
                provider_kwargs["api_key"] = api_key
            if base_url:
                provider_kwargs["base_url"] = base_url
            if organization:
                provider_kwargs["organization"] = organization
            
            if provider_kwargs:
                openai_provider = OpenAIProvider(**provider_kwargs)
                return OpenAIChatModel(model_name, provider=openai_provider, **model_kwargs)
            else:
                # No custom provider needed, use default
                return OpenAIChatModel(model_name, **model_kwargs)
        
        elif provider == "anthropic":
            from pydantic_ai.models.anthropic import AnthropicModel
            from pydantic_ai.providers.anthropic import AnthropicProvider
            
            # Extract API key and other settings
            api_key = model_kwargs.pop("api_key", None)
            base_url = model_kwargs.pop("base_url", None)
            
            # Create provider with API key and settings
            if api_key or base_url:
                provider_kwargs = {}
                if api_key:
                    provider_kwargs["api_key"] = api_key
                if base_url:
                    provider_kwargs["base_url"] = base_url
                anthropic_provider = AnthropicProvider(**provider_kwargs)
                return AnthropicModel(model_name, provider=anthropic_provider, **model_kwargs)
            else:
                # No custom provider needed, use default
                return AnthropicModel(model_name, **model_kwargs)
        
        elif provider == "google":
            from pydantic_ai.models.google import GoogleModel
            from pydantic_ai.providers.google import GoogleProvider
            
            # Extract API key
            api_key = model_kwargs.pop("api_key", None)
            
            # Create provider with API key
            if api_key:
                google_provider = GoogleProvider(api_key=api_key)
                return GoogleModel(model_name, provider=google_provider, **model_kwargs)
            else:
                # No custom provider needed, use default
                return GoogleModel(model_name, **model_kwargs)
        
        elif provider == "ollama":
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.ollama import OllamaProvider
            
            # Extract base_url if provided
            base_url = model_kwargs.pop("base_url", None) or args.get("ollama_base_url")
            
            # Create Ollama provider with base_url
            if base_url:
                ollama_provider = OllamaProvider(base_url=base_url)
                return OpenAIChatModel(model_name, provider=ollama_provider, **model_kwargs)
            else:
                # Default Ollama base_url
                ollama_provider = OllamaProvider()
                return OpenAIChatModel(model_name, provider=ollama_provider, **model_kwargs)
        
        elif provider == "nvidia_nim":
            # NVIDIA NIM uses OpenAI-compatible API
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider
            
            # Extract API key and base_url
            api_key = model_kwargs.pop("api_key", None)
            base_url = args.get("nvidia_nim_base_url") or args.get("base_url") or model_kwargs.pop("base_url", None)
            
            # Create provider with base_url for NVIDIA NIM
            provider_kwargs = {}
            if api_key:
                provider_kwargs["api_key"] = api_key
            if base_url:
                provider_kwargs["base_url"] = base_url
            
            if provider_kwargs:
                nim_provider = OpenAIProvider(**provider_kwargs)
                return OpenAIChatModel(model_name, provider=nim_provider, **model_kwargs)
            else:
                return OpenAIChatModel(model_name, **model_kwargs)
        
        elif provider == "writer":
            # Writer might need custom handling or OpenAI-compatible API
            raise ValueError(f"Writer provider not yet supported for model instances")
        
        elif provider == "litellm":
            # LiteLLM uses OpenAI-compatible API
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider
            
            # Extract API key and base_url
            api_key = model_kwargs.pop("api_key", None)
            base_url = args.get("litellm_api_base") or args.get("base_url") or model_kwargs.pop("base_url", None)
            
            # Create provider with base_url for LiteLLM
            provider_kwargs = {}
            if api_key:
                provider_kwargs["api_key"] = api_key
            if base_url:
                provider_kwargs["base_url"] = base_url
            
            if provider_kwargs:
                litellm_provider = OpenAIProvider(**provider_kwargs)
                return OpenAIChatModel(model_name, provider=litellm_provider, **model_kwargs)
            else:
                return OpenAIChatModel(model_name, **model_kwargs)
        
        elif provider == "bedrock":
            # AWS Bedrock might need custom wrapper
            raise ValueError(f"Bedrock provider not yet supported for model instances")
        
        elif provider == "mindsdb":
            # MindsDB custom provider - not yet supported for model instances
            raise ValueError("MindsDB provider is not yet supported for model instances. Please use a different provider.")
        
        else:
            raise ValueError(f"Unknown provider: {provider}")
    
    except ImportError as e:
        logger.error(f"Failed to import Pydantic AI model class for provider {provider}: {e}")
        raise ImportError(
            f"Pydantic AI model class for provider '{provider}' is not available. "
            f"Please ensure pydantic-ai is installed with the required dependencies."
        ) from e
    except Exception as e:
        logger.error(f"Error creating model instance for provider {provider}: {e}", exc_info=True)
        raise ValueError(f"Failed to create model instance: {str(e)}") from e

