from enum import Enum

from typing import List, Dict, Any, Optional, Union, Callable


from pydantic import BaseModel, Field, model_validator


from mindsdb.integrations.utilities.rag.settings import DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE
from mindsdb.integrations.utilities.rag.settings import LLMConfig


class PreprocessorType(Enum):
    CONTEXTUAL = "contextual"
    TEXT_CHUNKING = "text_chunking"


class BasePreprocessingConfig(BaseModel):
    """Base configuration for preprocessing"""
    chunk_size: int = Field(default=DEFAULT_CHUNK_SIZE, description="Size of document chunks")
    chunk_overlap: int = Field(default=DEFAULT_CHUNK_OVERLAP, description="Overlap between chunks")


class ContextualConfig(BasePreprocessingConfig):
    """Configuration specific to contextual preprocessing"""
    llm_config: LLMConfig = Field(
        default_factory=LLMConfig,
        description="LLM configuration to use for context generation"
    )
    context_template: Optional[str] = Field(
        default=None,
        description="Custom template for context generation"
    )
    summarize: Optional[bool] = Field(
        default=False,
        description="Whether to return chunks as summarizations"
    )


class TextChunkingConfig(BaseModel):
    """Configuration for text chunking preprocessor using Pydantic"""
    chunk_size: int = Field(
        default=1000,
        description="The target size of each text chunk",
        gt=0
    )
    chunk_overlap: int = Field(
        default=200,
        description="The number of characters to overlap between chunks",
        ge=0
    )
    length_function: Callable = Field(
        default=len,
        description="Function to measure text length"
    )
    separators: List[str] = Field(
        default=["\n\n", "\n", " ", ""],
        description="List of separators to use for splitting text, in order of priority"
    )

    class Config:
        arbitrary_types_allowed = True


class PreprocessingConfig(BaseModel):
    """Complete preprocessing configuration"""
    type: PreprocessorType = Field(
        default=PreprocessorType.TEXT_CHUNKING,
        description="Type of preprocessing to apply"
    )
    contextual_config: Optional[ContextualConfig] = Field(
        default=None,
        description="Configuration for contextual preprocessing"
    )
    text_chunking_config: Optional[TextChunkingConfig] = Field(
        default=None,
        description="Configuration for text chunking preprocessing"
    )

    @model_validator(mode='after')
    def validate_config_presence(self) -> 'PreprocessingConfig':
        """Ensure the appropriate config is present for the chosen type"""
        if self.type == PreprocessorType.CONTEXTUAL and not self.contextual_config:
            self.contextual_config = ContextualConfig()
        if self.type == PreprocessorType.TEXT_CHUNKING and not self.text_chunking_config:
            self.text_chunking_config = TextChunkingConfig()
        return self


class Document(BaseModel):

    """Document model with default metadata handling"""
    id: Optional[Union[int, str]] = Field(default=None, description="Unique identifier for the document")
    content: str = Field(description="The document content")
    embeddings: Optional[List[float]] = Field(default=None, description="Vector embeddings of the content")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional document metadata")

    @model_validator(mode='after')
    def validate_metadata(self) -> 'Document':
        """Ensure metadata is present and valid"""
        if not self.metadata:
            self.metadata = {'source': 'default'}
        return self


class ProcessedChunk(Document):
    """Processed chunk that aligns with VectorStoreHandler schema"""
    pass
