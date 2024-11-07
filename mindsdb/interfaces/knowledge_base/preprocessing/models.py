from enum import Enum
from typing import List, Dict, Any, Optional, Union

from pydantic import BaseModel, Field, model_validator

from mindsdb.integrations.utilities.rag.settings import DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE
from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_MODEL


class PreprocessorType(str, Enum):
    """Types of preprocessors available"""
    CONTEXTUAL = "contextual"


class BasePreprocessingConfig(BaseModel):
    """Base configuration for preprocessing"""
    chunk_size: int = Field(default=DEFAULT_CHUNK_SIZE, description="Size of document chunks")
    chunk_overlap: int = Field(default=DEFAULT_CHUNK_OVERLAP, description="Overlap between chunks")


class ContextualConfig(BasePreprocessingConfig):
    """Configuration specific to contextual preprocessing"""
    llm_model: str = Field(default=DEFAULT_LLM_MODEL, description="LLM model to use for context generation")
    context_template: Optional[str] = Field(
        default=None,
        description="Custom template for context generation"
    )


class PreprocessingConfig(BaseModel):
    """Complete preprocessing configuration"""
    type: PreprocessorType = Field(
        description="Type of preprocessing to apply"
    )
    contextual_config: Optional[ContextualConfig] = Field(
        default=None,
        description="Configuration for contextual preprocessing"
    )

    @model_validator(mode='after')
    def validate_config_presence(self) -> 'PreprocessingConfig':
        """Ensure the appropriate config is present for the chosen type"""
        if self.type == PreprocessorType.CONTEXTUAL and not self.contextual_config:
            self.contextual_config = ContextualConfig()
        return self


class Document(BaseModel):
    """Base document model for knowledge base operations"""
    id: Optional[Union[int, str]] = Field(default=None, description="Unique identifier for the document")
    content: str = Field(description="The document content")
    embeddings: Optional[List[float]] = Field(default=None, description="Vector embeddings of the content")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional document metadata")

    class Config:
        arbitrary_types_allowed = True


class ProcessedChunk(Document):
    """Processed chunk that aligns with VectorStoreHandler schema"""
    pass
