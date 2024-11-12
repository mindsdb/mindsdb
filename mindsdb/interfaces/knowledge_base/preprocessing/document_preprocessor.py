from typing import List, Dict, Optional, Any
import pandas as pd

from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.agents.langchain_agent import create_chat_model
from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    PreprocessingConfig,
    ProcessedChunk,
    PreprocessorType,
    ContextualConfig,
    Document
)
from mindsdb.utilities import log

from langchain_core.documents import Document as LangchainDocument

logger = log.getLogger(__name__)


class DocumentPreprocessor:
    """Base class for document preprocessing"""
    RESERVED_METADATA_FIELDS = {'content', 'id', 'embeddings'}

    def __init__(self, preprocessing_config: Optional[PreprocessingConfig] = None):
        """Initialize preprocessor with optional configuration"""
        self.preprocessor = PreprocessorFactory.create_preprocessor(
            preprocessing_config) if preprocessing_config else None

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        """
        Process documents through configured preprocessor
        : param documents: List of Document objects to process
        : return: List of processed chunks
        """
        if self.preprocessor:
            return self.preprocessor.process_documents(documents)
        # If no preprocessor configured, return documents as ProcessedChunks
        return [ProcessedChunk(
            id=doc.id,
            content=doc.content,
            embeddings=doc.embeddings,
            metadata=doc.metadata
        ) for doc in documents]

    def _prepare_metadata(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and prepare metadata from document dictionary"""
        base_metadata = {
            'source': doc.get('source'),
            'url': doc.get('url')
        }
        custom_metadata = {
            k: v for k, v in doc.items()
            if k not in self.RESERVED_METADATA_FIELDS and k not in base_metadata
        }
        base_metadata.update(custom_metadata)
        return {k: v for k, v in base_metadata.items() if v is not None}

    def to_dataframe(self, chunks: List[ProcessedChunk]) -> pd.DataFrame:
        """Convert processed chunks to dataframe format"""
        return pd.DataFrame([chunk.model_dump() for chunk in chunks])

    @staticmethod
    def dict_to_document(data: Dict[str, Any]) -> Document:
        """Convert a dictionary to a Document object"""
        return Document(
            id=data.get('id'),
            content=data.get('content', ''),
            embeddings=data.get('embeddings'),
            metadata=data.get('metadata', {})
        )


class ContextualPreprocessor(DocumentPreprocessor):
    """Contextual preprocessing implementation that enhances document chunks with context"""

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
        """Initialize with contextual configuration"""
        super().__init__()
        self.config = config
        self.splitter = FileSplitter(FileSplitterConfig(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap
        ))
        self.llm = create_chat_model({"model_name": config.llm_model})
        self.context_template = config.context_template or self.DEFAULT_CONTEXT_TEMPLATE

    def _generate_context(self, chunk_content: str, full_document: str) -> str:
        """Generate contextual description for a chunk using LLM"""
        prompt = self.context_template.replace("{{WHOLE_DOCUMENT}}", full_document)
        prompt = prompt.replace("{{CHUNK_CONTENT}}", chunk_content)
        response = self.llm(prompt)
        return response.content

    def _split_document(self, doc: Document) -> List[Document]:
        """Split document into chunks while preserving metadata"""
        # Convert to langchain Document for splitting
        langchain_doc = LangchainDocument(
            page_content=doc.content,
            metadata=doc.metadata or {}
        )
        # Split and convert back to our Document type
        split_docs = self.splitter.split_documents([langchain_doc])
        return [Document(
            content=split_doc.page_content,
            metadata=split_doc.metadata
        ) for split_doc in split_docs]

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        """Process documents with contextual enhancement"""
        processed_chunks = []

        for doc in documents:
            # Split document into chunks
            chunk_docs = self._split_document(doc)

            # Process each chunk with context
            for chunk_doc in chunk_docs:
                context = self._generate_context(chunk_doc.content, doc.content)
                processed_content = f"{context}\n\n{chunk_doc.content}"

                processed_chunks.append(ProcessedChunk(
                    id=doc.id,
                    content=processed_content,
                    embeddings=doc.embeddings,
                    metadata=chunk_doc.metadata or doc.metadata
                ))

        return processed_chunks


class PreprocessorFactory:
    """Factory for creating preprocessors based on configuration"""

    @staticmethod
    def create_preprocessor(config: PreprocessingConfig) -> DocumentPreprocessor:
        """
        Create appropriate preprocessor based on configuration
        : param config: Preprocessing configuration
        : return: Configured preprocessor instance
        : raises ValueError: If unknown preprocessor type specified
        """
        if config.type == PreprocessorType.CONTEXTUAL:
            return ContextualPreprocessor(config.contextual_config or ContextualConfig())
        raise ValueError(f"Unknown preprocessor type: {config.type}")
