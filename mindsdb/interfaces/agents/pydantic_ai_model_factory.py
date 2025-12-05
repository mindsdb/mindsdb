"""Pydantic AI model factory to create models from MindsDB configuration"""

from typing import Dict, Any, Optional
from mindsdb.utilities import log
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.libs.llm.utils import get_llm_config
from mindsdb.interfaces.agents.constants import (
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


def create_pydantic_ai_model(args: Dict[str, Any]) -> str:
    """
    Create a Pydantic AI model string from MindsDB args.
    
    Args:
        args: Dictionary containing model configuration (provider, model_name, api keys, etc.)
        
    Returns:
        Model string in format expected by Pydantic AI (e.g., "openai:gpt-4", "anthropic:claude-3-sonnet")
        
    Raises:
        ValueError: If provider is not supported or model name is invalid
    """
    # Determine provider
    provider = args.get("provider")
    if provider is None:
        provider = get_llm_provider(args)
    
    model_name = args.get("model_name")
    if not model_name:
        raise ValueError("model_name is required")
    
    # Map MindsDB providers to Pydantic AI model strings
    # Pydantic AI uses format: "provider:model-name" or "gateway/provider:model-name"
    # For now, we'll use direct provider format
    
    if provider == "openai" or provider == "vllm":
        # OpenAI models
        if model_name in OPEN_AI_CHAT_MODELS:
            return f"openai:{model_name}"
        # For vLLM, we might need custom handling
        if provider == "vllm":
            # vLLM typically uses OpenAI-compatible API
            base_url = args.get("openai_api_base")
            if base_url:
                # For vLLM, we may need a custom model wrapper
                # For now, return OpenAI format - will need custom handling
                return f"openai:{model_name}"
            return f"openai:{model_name}"
        return f"openai:{model_name}"
    
    elif provider == "anthropic":
        if model_name in ANTHROPIC_CHAT_MODELS:
            return f"anthropic:{model_name}"
        return f"anthropic:{model_name}"
    
    elif provider == "google":
        if model_name in GOOGLE_GEMINI_CHAT_MODELS:
            # Google models use google-gla prefix in Pydantic AI
            return f"google-gla:{model_name}"
        return f"google-gla:{model_name}"
    
    elif provider == "ollama":
        if model_name in OLLAMA_CHAT_MODELS:
            # Ollama uses ollama: prefix
            return f"ollama:{model_name}"
        return f"ollama:{model_name}"
    
    elif provider == "nvidia_nim":
        if model_name in NVIDIA_NIM_CHAT_MODELS:
            # NVIDIA NIM might need custom handling
            return f"nvidia-nim:{model_name}"
        return f"nvidia-nim:{model_name}"
    
    elif provider == "writer":
        if model_name in WRITER_CHAT_MODELS:
            # Writer models might need custom wrapper
            # For now, return a placeholder - will need custom implementation
            raise ValueError(f"Writer provider not yet supported in Pydantic AI")
    
    elif provider == "litellm":
        # LiteLLM might need custom handling
        # For now, try to use OpenAI format if it's an OpenAI-compatible model
        return f"openai:{model_name}"
    
    elif provider == "bedrock":
        # AWS Bedrock might need custom wrapper
        raise ValueError(f"Bedrock provider not yet supported in Pydantic AI")
    
    elif provider == "mindsdb":
        # MindsDB custom provider - will need custom wrapper
        # Return a special marker that will be handled separately
        return "mindsdb:custom"
    
    else:
        raise ValueError(f"Unknown provider: {provider}")


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

