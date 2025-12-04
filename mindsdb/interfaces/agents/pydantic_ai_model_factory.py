"""Pydantic AI model factory to create models from MindsDB configuration"""

from typing import Dict, Any, Optional
from mindsdb.utilities import log
from mindsdb.interfaces.agents.langchain_agent import (
    get_llm_provider,
    get_chat_model_params,
    get_embedding_model_provider,
)
from mindsdb.interfaces.agents.constants import (
    OPEN_AI_CHAT_MODELS,
    ANTHROPIC_CHAT_MODELS,
    GOOGLE_GEMINI_CHAT_MODELS,
    OLLAMA_CHAT_MODELS,
    NVIDIA_NIM_CHAT_MODELS,
    WRITER_CHAT_MODELS,
)

logger = log.getLogger(__name__)


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
    
    Args:
        args: Dictionary containing model configuration
        
    Returns:
        Dictionary of keyword arguments for Pydantic AI model
    """
    kwargs = {}
    
    # Get provider
    provider = args.get("provider")
    if provider is None:
        provider = get_llm_provider(args)
    
    # Extract API keys based on provider
    if provider == "openai" or provider == "vllm":
        api_key = args.get("openai_api_key") or args.get("api_key")
        if api_key:
            kwargs["api_key"] = api_key
        base_url = args.get("openai_api_base") or args.get("base_url")
        if base_url:
            kwargs["base_url"] = base_url
        organization = args.get("openai_api_organization") or args.get("api_organization")
        if organization:
            kwargs["organization"] = organization
    
    elif provider == "anthropic":
        api_key = args.get("anthropic_api_key") or args.get("api_key")
        if api_key:
            kwargs["api_key"] = api_key
        base_url = args.get("anthropic_api_url") or args.get("base_url")
        if base_url:
            kwargs["base_url"] = base_url
    
    elif provider == "google":
        api_key = args.get("google_api_key") or args.get("api_key")
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

