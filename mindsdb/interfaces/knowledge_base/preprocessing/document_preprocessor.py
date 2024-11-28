from typing import List, Dict, Optional, Any
import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter
import hashlib

from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.agents.langchain_agent import create_chat_model
from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    PreprocessingConfig,
    ProcessedChunk,
    PreprocessorType,
    ContextualConfig,
    Document, TextChunkingConfig
)
from mindsdb.utilities import log

from langchain_core.documents import Document as LangchainDocument

logger = log.getLogger(__name__)


class DocumentPreprocessor:
    """Base class for document preprocessing"""
    RESERVED_METADATA_FIELDS = {'content', 'id', 'embeddings', 'original_doc_id', 'chunk_index'}

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

    @staticmethod
    def _generate_deterministic_id(content: str, content_column: str = None) -> str:
        """Generate a deterministic ID based on content and column"""
        hash_input = f"{content_column}:{content}" if content_column else content
        return hashlib.sha256(hash_input.encode()).hexdigest()

    def _generate_chunk_id(self, content: str, chunk_index: Optional[int] = None, content_column: str = None) -> str:
        """Generate deterministic ID for a chunk"""
        base_id = self._generate_deterministic_id(content, content_column)
        if chunk_index is None:
            return base_id
        return f"{base_id}_chunk_{chunk_index}"

    def _prepare_chunk_metadata(self,
                                doc_id: Optional[str],
                                chunk_index: Optional[int],
                                base_metadata: Optional[Dict] = None) -> Dict:
        """Centralized method for preparing chunk metadata"""
        metadata = base_metadata or {}
        if chunk_index is not None:
            metadata['original_doc_id'] = doc_id
            metadata['chunk_index'] = chunk_index
        return metadata


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
        self.llm = create_chat_model({
            "model_name": self.config.llm_config.model_name,
            "provider": self.config.llm_config.provider,
            **self.config.llm_config.params
        })
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
        processed_chunks = []

        for doc in documents:
            chunk_docs = self._split_document(doc)

            # Single chunk case
            if len(chunk_docs) == 1:
                context = self._generate_context(chunk_docs[0].content, doc.content)
                processed_content = f"{context}\n\n{chunk_docs[0].content}"

                processed_chunks.append(ProcessedChunk(
                    # Use original doc ID since there's only one chunk
                    id=doc.id or self._generate_chunk_id(processed_content),
                    content=processed_content,
                    embeddings=doc.embeddings,
                    metadata=self._prepare_chunk_metadata(doc.id, None, chunk_docs[0].metadata or doc.metadata)
                ))
            else:
                # Multiple chunks case
                for i, chunk_doc in enumerate(chunk_docs):
                    context = self._generate_context(chunk_doc.content, doc.content)
                    processed_content = f"{context}\n\n{chunk_doc.content}"

                    # Append chunk index to original doc ID
                    chunk_id = f"{doc.id}_chunk_{i}" if doc.id else self._generate_chunk_id(processed_content, i)

                    processed_chunks.append(ProcessedChunk(
                        id=chunk_id,
                        content=processed_content,
                        embeddings=doc.embeddings,
                        metadata=self._prepare_chunk_metadata(doc.id, i, chunk_doc.metadata or doc.metadata)
                    ))

        return processed_chunks


class TextChunkingPreprocessor(DocumentPreprocessor):
    """Default text chunking preprocessor using RecursiveCharacterTextSplitter"""

    def __init__(self, config: Optional[TextChunkingConfig] = None):
        """Initialize with text chunking configuration"""
        super().__init__()
        self.config = config or TextChunkingConfig()
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.config.chunk_size,
            chunk_overlap=self.config.chunk_overlap,
            length_function=self.config.length_function,
            separators=self.config.separators
        )

    def _split_document(self, doc: Document) -> List[Document]:
        """Split document into chunks while preserving metadata"""
        langchain_doc = LangchainDocument(
            page_content=doc.content,
            metadata=doc.metadata or {}
        )
        split_docs = self.splitter.split_documents([langchain_doc])
        return [Document(
            content=split_doc.page_content,
            metadata=split_doc.metadata
        ) for split_doc in split_docs]

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        processed_chunks = []

        for doc in documents:
            # Skip empty or whitespace-only content
            if not doc.content or not doc.content.strip():
                continue

            chunk_docs = self._split_document(doc)

            # Single chunk case - use original ID
            if len(chunk_docs) == 1:
                chunk_doc = chunk_docs[0]
                if not chunk_doc.content or not chunk_doc.content.strip():
                    continue

                metadata = {"source": "default"}
                if doc.metadata:
                    metadata.update(doc.metadata)

                processed_chunks.append(ProcessedChunk(
                    id=doc.id or self._generate_chunk_id(chunk_doc.content),
                    content=chunk_doc.content,
                    embeddings=doc.embeddings,
                    metadata=self._prepare_chunk_metadata(doc.id, None, metadata)
                ))
            else:
                # Multiple chunks case - append chunk index to original ID
                for i, chunk_doc in enumerate(chunk_docs):
                    if not chunk_doc.content or not chunk_doc.content.strip():
                        continue

                    metadata = {"source": "default"}
                    if doc.metadata:
                        metadata.update(doc.metadata)

                    chunk_id = f"{doc.id}_chunk_{i}" if doc.id else self._generate_chunk_id(chunk_doc.content, i)

                    processed_chunks.append(ProcessedChunk(
                        id=chunk_id,
                        content=chunk_doc.content,
                        embeddings=doc.embeddings,
                        metadata=self._prepare_chunk_metadata(doc.id, i, metadata)
                    ))

        return processed_chunks


class PreprocessorFactory:
    """Factory for creating preprocessors based on configuration"""

    @staticmethod
    def create_preprocessor(config: Optional[PreprocessingConfig] = None) -> DocumentPreprocessor:
        """
        Create appropriate preprocessor based on configuration
        : param config: Preprocessing configuration
        : return: Configured preprocessor instance
        : raises ValueError: If unknown preprocessor type specified
        """
        if config is None:
            return TextChunkingPreprocessor()

        if config.type == PreprocessorType.CONTEXTUAL:
            return ContextualPreprocessor(config.contextual_config or ContextualConfig())
        elif config.type == PreprocessorType.TEXT_CHUNKING:
            return TextChunkingPreprocessor(config.text_chunking_config)

        raise ValueError(f"Unknown preprocessor type: {config.type}")
