from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class BaseLLMConfig(BaseModel):
    # Remove 'model_' prefix from protected namespaces since Langchain constructor
    # kwargs share the same prefix.
    model_config = ConfigDict(protected_namespaces=())


# See https://api.python.langchain.com/en/latest/chat_models/langchain_community.chat_models.openai.ChatOpenAI.html#langchain_community.chat_models.openai.ChatOpenAI
# This config does not have to be exclusively used with Langchain.
class OpenAIConfig(BaseLLMConfig):
    model_name: str
    temperature: Optional[float]
    max_retries: Optional[int]
    max_tokens: Optional[int]
    openai_api_base: Optional[str]
    # Inferred from OPENAI_API_KEY if not provided.
    openai_api_key: Optional[str]
    openai_organization: Optional[str]
    request_timeout: Optional[float]


# See https://api.python.langchain.com/en/latest/chat_models/langchain_community.chat_models.anthropic.ChatAnthropic.html
# This config does not have to be exclusively used with Langchain.
class AnthropicConfig(BaseLLMConfig):
    model: str
    temperature: Optional[float]
    max_tokens: Optional[int]
    top_p: Optional[float]
    top_k: Optional[int]
    default_request_timeout: Optional[float]
    # Inferred from ANTHROPIC_API_KEY if not provided.
    anthropic_api_key: Optional[str]
    anthropic_api_url: Optional[str]


# See https://api.python.langchain.com/en/latest/chat_models/langchain_community.chat_models.anyscale.ChatAnyscale.html
# This config does not have to be exclusively used with Langchain.
class AnyscaleConfig(BaseLLMConfig):
    model_name: str
    temperature: Optional[float]
    max_retries: Optional[int]
    max_tokens: Optional[int]
    anyscale_api_base: Optional[str]
    # Inferred from ANYSCALE_API_KEY if not provided.
    anyscale_api_key: Optional[str]
    anyscale_proxy: Optional[str]
    request_timeout: Optional[float]


# See https://api.python.langchain.com/en/latest/chat_models/langchain_community.chat_models.litellm.ChatLiteLLM.html
# This config does not have to be exclusively used with Langchain.
class LiteLLMConfig(BaseLLMConfig):
    model: str
    api_base: Optional[str]
    max_retries: Optional[int]
    max_tokens: Optional[int]
    top_p: Optional[float]
    top_k: Optional[int]
    temperature: Optional[float]
    custom_llm_provider: Optional[str]
    model_kwargs: Optional[Dict[str, Any]]


# See https://api.python.langchain.com/en/latest/chat_models/langchain_community.chat_models.ollama.ChatOllama.html
# This config does not have to be exclusively used with Langchain.
class OllamaConfig(BaseLLMConfig):
    base_url: str
    model: str
    temperature: Optional[float]
    top_p: Optional[float]
    top_k: Optional[int]
    timeout: Optional[int]
    format: Optional[str]
    headers: Optional[Dict]
    num_predict: Optional[int]
    num_ctx: Optional[int]
    num_gpu: Optional[int]
    repeat_penalty: Optional[float]
    stop: Optional[List[str]]
    template: Optional[str]


class NvidiaNIMConfig(BaseLLMConfig):
    base_url: str
    model: str
    temperature: Optional[float]
    top_p: Optional[float]
    timeout: Optional[int]
    format: Optional[str]
    headers: Optional[Dict]
    num_predict: Optional[int]
    num_ctx: Optional[int]
    num_gpu: Optional[int]
    repeat_penalty: Optional[float]
    stop: Optional[List[str]]
    template: Optional[str]
    nvidia_api_key: Optional[str]


class MindsdbConfig(BaseLLMConfig):
    model_name: str
    project_name: str
