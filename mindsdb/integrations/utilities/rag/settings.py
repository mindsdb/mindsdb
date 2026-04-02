from enum import Enum
from typing import List, Any, Optional, Dict

from pydantic import BaseModel, Field, ConfigDict


DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 200
DEFAULT_LLM_MODEL = "gpt-4o"
DEFAULT_LLM_ENDPOINT = "https://api.openai.com/v1"
DEFAULT_LLM_MODEL_PROVIDER = "openai"
DEFAULT_RERANKING_MODEL = "gpt-4o"
DEFAULT_RERANKER_N = 1
DEFAULT_RERANKER_LOGPROBS = True
DEFAULT_RERANKER_TOP_LOGPROBS = 4
DEFAULT_RERANKER_MAX_TOKENS = 100
DEFAULT_VALID_CLASS_TOKENS = ["1", "2", "3", "4"]


class LLMConfig(BaseModel):
    model_name: str = Field(default=DEFAULT_LLM_MODEL, description="LLM model to use for generation")
    provider: str = Field(
        default=DEFAULT_LLM_MODEL_PROVIDER,
        description="LLM model provider to use for generation",
    )
    params: Dict[str, Any] = Field(default_factory=dict)
    model_config = ConfigDict(protected_namespaces=())


class RerankerMode(str, Enum):
    POINTWISE = "pointwise"
    LISTWISE = "listwise"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            value = value.lower()
            for member in cls:
                if member.value == value:
                    return member
        return None


class RerankerConfig(BaseModel):
    model: str = DEFAULT_RERANKING_MODEL
    base_url: Optional[str] = None
    filtering_threshold: float = 0.5
    num_docs_to_keep: Optional[int] = None
    mode: RerankerMode = Field(
        default=RerankerMode.POINTWISE,
        description="Reranking mode to use. 'pointwise' for individual scoring, '"
        "listwise' for joint scoring of all documents.",
    )
    max_concurrent_requests: int = 20
    max_retries: int = 3
    retry_delay: float = 1.0
    early_stop: bool = True  # Whether to enable early stopping
    early_stop_threshold: float = 0.8  # Confidence threshold for early stopping
    n: int = DEFAULT_RERANKER_N  # Number of completions to generate
    logprobs: bool = DEFAULT_RERANKER_LOGPROBS  # Whether to include log probabilities
    top_logprobs: int = DEFAULT_RERANKER_TOP_LOGPROBS  # Number of top log probabilities to include
    max_tokens: int = DEFAULT_RERANKER_MAX_TOKENS  # Maximum tokens to generate
    valid_class_tokens: List[str] = DEFAULT_VALID_CLASS_TOKENS  # Valid class tokens to look for in the response
