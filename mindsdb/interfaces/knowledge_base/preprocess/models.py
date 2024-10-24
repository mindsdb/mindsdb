from typing import List, Dict, Any, Optional, Sequence
from enum import Enum

from langchain_core.documents import Document
from pydantic import BaseModel, Field, model_validator

from mindsdb.integrations.utilities.rag.settings import DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE
from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_MODEL
from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.agents.langchain_agent import create_chat_model


class TableField(str, Enum):
    """Matches VectorStoreHandler schema fields"""
    ID = "id"
    CONTENT = "content"
    EMBEDDINGS = "embeddings"
    METADATA = "metadata"


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


class ProcessedChunk(BaseModel):
    """Processed chunk that aligns with VectorStoreHandler schema"""
    id: Optional[str] = None
    content: str
    embeddings: Optional[List[float]] = None
    metadata: Optional[Dict[str, Any]] = None


class DocumentPreprocessor:
    """Base class for document preprocessors"""
    RESERVED_METADATA_FIELDS = {'content', 'id', 'embeddings'}

    def process_documents(self, documents: List[Dict[str, Any]]) -> List[ProcessedChunk]:
        raise NotImplementedError()

    def _prepare_metadata(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and combine metadata from document, excluding reserved fields"""
        base_metadata = {
            'source': doc.get('source'),
            'url': doc.get('url')
        }
        custom_metadata = {
            k: v for k, v in doc.items()
            if k not in self.RESERVED_METADATA_FIELDS and k not in base_metadata
        }
        base_metadata.update(custom_metadata)
        return base_metadata

    @staticmethod
    def _create_document(content: str, metadata: Dict[str, Any] = None) -> Document:
        """Create a langchain Document with content and metadata"""
        return Document(page_content=content, metadata=metadata or {})


class ContextualPreprocessor(DocumentPreprocessor):
    """Contextual preprocessing implementation"""

    DEFAULT_CONTEXT_TEMPLATE = """
<document>
{{WHOLE_DOCUMENT}}
</document>
Here is the chunk we want to situate within the whole document
<chunk>
{{CHUNK_CONTENT}}
</chunk>
Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk. Answer only with the succinct context and nothing else."""

    def __init__(self, config: ContextualConfig):
        self.config = config
        self.splitter = FileSplitter(FileSplitterConfig(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap
        ))
        self.llm = create_chat_model({"model_name": config.llm_model})
        self.context_template = config.context_template or self.DEFAULT_CONTEXT_TEMPLATE

    def _generate_context(self, chunk: str, full_document: str) -> str:
        """Generate contextual description for a chunk using LLM"""
        prompt = self.context_template.replace("{{WHOLE_DOCUMENT}}", full_document)
        prompt = prompt.replace("{{CHUNK_CONTENT}}", chunk)
        response = self.llm(prompt)
        return response.content

    def _split_document(self, doc: Document) -> Sequence[Document]:
        """Split document into chunks while preserving metadata"""
        return self.splitter.split_documents([doc])

    def process_documents(self, documents: List[Dict[str, Any]]) -> List[ProcessedChunk]:
        processed_chunks = []
        for doc in documents:
            text = doc.get('content', '')
            metadata = self._prepare_metadata(doc)

            # Create and split document
            langchain_doc = self._create_document(text, metadata)
            chunk_docs = self._split_document(langchain_doc)

            for chunk_doc in chunk_docs:
                context = self._generate_context(chunk_doc.page_content, text)
                processed_content = f"{context}\n\n{chunk_doc.page_content}"

                processed_chunks.append(ProcessedChunk(
                    id=doc.get('id'),
                    content=processed_content,
                    embeddings=doc.get('embeddings'),
                    metadata=chunk_doc.metadata  # Use metadata from split document
                ))

        return processed_chunks


class PreprocessorFactory:
    """Factory for creating preprocessors based on configuration"""

    @staticmethod
    def create_preprocessor(config: PreprocessingConfig) -> DocumentPreprocessor:
        if config.type == PreprocessorType.CONTEXTUAL:
            return ContextualPreprocessor(config.contextual_config or ContextualConfig())
        raise ValueError(f"Unknown preprocessor type: {config.type}")
