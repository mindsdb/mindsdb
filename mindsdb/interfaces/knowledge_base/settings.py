from pydantic import BaseModel, Field
from typing import Optional

_DEFAULT_LLM_MODEL = "gpt-4o"
_DEFAULT_LLM_PROVIDER = "openai"
_DEFAULT_MAX_CHUNK_SIZE = 4000
_DEFAULT_MAX_CHUNK_OVERLAP = 200


class SummarizationParams(BaseModel):
    llm_provider: str = Field(default=_DEFAULT_LLM_MODEL, description="The LLM provider to use for summarization")
    model_name: str = Field(default=_DEFAULT_LLM_MODEL, description="The specific model to use for summarization")
    chunk_size: int = Field(default=_DEFAULT_MAX_CHUNK_SIZE, description="The size of text chunks for summarization")
    chunk_overlap: int = Field(default=_DEFAULT_MAX_CHUNK_OVERLAP, description="The overlap between text chunks")
    prompt_template: Optional[str] = Field(default=None, description="Custom prompt template for summarization")

    class Config:
        extra = "allow"
