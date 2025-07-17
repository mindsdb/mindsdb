from enum import Enum

from typing import List, Dict, Any, Optional, Union, Callable


from pydantic import BaseModel, Field, model_validator


from mindsdb.integrations.utilities.rag.settings import DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE
from mindsdb.integrations.utilities.rag.settings import LLMConfig


class PreprocessorType(Enum):
    CONTEXTUAL = "contextual"
    TEXT_CHUNKING = "text_chunking"
    JSON_CHUNKING = "json_chunking"


class BasePreprocessingConfig(BaseModel):
    """Base configuration for preprocessing"""

    chunk_size: int = Field(default=DEFAULT_CHUNK_SIZE, description="Size of document chunks")
    chunk_overlap: int = Field(default=DEFAULT_CHUNK_OVERLAP, description="Overlap between chunks")
    doc_id_column_name: str = Field(default="_original_doc_id", description="Name of doc_id columns in metadata")


class ContextualConfig(BasePreprocessingConfig):
    """Configuration specific to contextual preprocessing"""

    llm_config: LLMConfig = Field(
        default_factory=LLMConfig, description="LLM configuration to use for context generation"
    )
    context_template: Optional[str] = Field(default=None, description="Custom template for context generation")
    summarize: Optional[bool] = Field(default=False, description="Whether to return chunks as summarizations")


class TextChunkingConfig(BasePreprocessingConfig):
    """Configuration for text chunking preprocessor using Pydantic"""

    chunk_size: int = Field(default=1000, description="The target size of each text chunk", gt=0)
    chunk_overlap: int = Field(default=200, description="The number of characters to overlap between chunks", ge=0)
    length_function: Callable = Field(default=len, description="Function to measure text length")
    separators: List[str] = Field(
        default=["\n\n", "\n", " ", ""],
        description="List of separators to use for splitting text, in order of priority",
    )

    class Config:
        arbitrary_types_allowed = True


class JSONChunkingConfig(BasePreprocessingConfig):
    """Configuration for JSON chunking preprocessor"""

    flatten_nested: bool = Field(default=True, description="Whether to flatten nested JSON structures")
    include_metadata: bool = Field(default=True, description="Whether to include original metadata in chunks")
    chunk_by_object: bool = Field(
        default=True, description="Whether to chunk by top-level objects (True) or create a single document (False)"
    )
    exclude_fields: List[str] = Field(default_factory=list, description="List of fields to exclude from chunking")
    include_fields: List[str] = Field(
        default_factory=list,
        description="List of fields to include in chunking (if empty, all fields except excluded ones are included)",
    )
    metadata_fields: List[str] = Field(
        default_factory=list,
        description="List of fields to extract into metadata for filtering "
        "(can include nested fields using dot notation). "
        "If empty, all primitive fields will be extracted (top-level fields if available, otherwise all primitive fields in the flattened structure).",
    )
    extract_all_primitives: bool = Field(
        default=False, description="Whether to extract all primitive values (strings, numbers, booleans) into metadata"
    )
    nested_delimiter: str = Field(default=".", description="Delimiter for flattened nested field names")
    content_column: str = Field(default="content", description="Name of the content column for chunk ID generation")

    class Config:
        arbitrary_types_allowed = True


class PreprocessingConfig(BaseModel):
    """Complete preprocessing configuration"""

    type: PreprocessorType = Field(default=PreprocessorType.TEXT_CHUNKING, description="Type of preprocessing to apply")
    contextual_config: Optional[ContextualConfig] = Field(
        default=None, description="Configuration for contextual preprocessing"
    )
    text_chunking_config: Optional[TextChunkingConfig] = Field(
        default=None, description="Configuration for text chunking preprocessing"
    )
    json_chunking_config: Optional[JSONChunkingConfig] = Field(
        default=None, description="Configuration for JSON chunking preprocessing"
    )

    @model_validator(mode="after")
    def validate_config_presence(self) -> "PreprocessingConfig":
        """Ensure the appropriate config is present for the chosen type"""
        if self.type == PreprocessorType.CONTEXTUAL and not self.contextual_config:
            self.contextual_config = ContextualConfig()
        if self.type == PreprocessorType.TEXT_CHUNKING and not self.text_chunking_config:
            self.text_chunking_config = TextChunkingConfig()
        if self.type == PreprocessorType.JSON_CHUNKING and not self.json_chunking_config:
            # Import here to avoid circular imports
            from mindsdb.interfaces.knowledge_base.preprocessing.json_chunker import JSONChunkingConfig

            self.json_chunking_config = JSONChunkingConfig()
        return self


class Document(BaseModel):
    """Document model with default metadata handling"""

    id: Optional[Union[int, str]] = Field(default=None, description="Unique identifier for the document")
    content: str = Field(description="The document content")
    embeddings: Optional[List[float]] = Field(default=None, description="Vector embeddings of the content")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional document metadata")

    @model_validator(mode="after")
    def validate_metadata(self) -> "Document":
        """Ensure metadata is present and valid"""
        if not self.metadata:
            self.metadata = {"source": "default"}
        return self


class ProcessedChunk(Document):
    """Processed chunk that aligns with VectorStoreHandler schema"""

    pass
