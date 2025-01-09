from typing import List, Dict, Optional, Any
import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter
import hashlib
import asyncio


from mindsdb.integrations.utilities.rag.splitters.file_splitter import (
    FileSplitter,
    FileSplitterConfig,
)

from mindsdb.interfaces.agents.langchain_agent import create_chat_model

from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    PreprocessingConfig,
    ProcessedChunk,
    PreprocessorType,
    ContextualConfig,
    Document,
    TextChunkingConfig,
)
from mindsdb.utilities import log

from langchain_core.documents import Document as LangchainDocument

logger = log.getLogger(__name__)


class DocumentPreprocessor:
    """Base class for document preprocessing"""

    RESERVED_METADATA_FIELDS = {
        "content",
        "id",
        "embeddings",
        "original_doc_id",
        "chunk_index",
    }

    def __init__(self):
        """Initialize preprocessor"""
        self.splitter = None  # Will be set by child classes

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        """Base implementation - should be overridden by child classes"""
        raise NotImplementedError("Subclasses must implement process_documents")

    def _split_document(self, doc: Document) -> List[Document]:
        """Split document into chunks while preserving metadata"""
        if self.splitter is None:
            raise ValueError("Splitter not configured")

        # Convert to langchain Document for splitting
        langchain_doc = LangchainDocument(
            page_content=doc.content, metadata=doc.metadata or {}
        )
        # Split and convert back to our Document type
        split_docs = self.splitter.split_documents([langchain_doc])
        return [
            Document(content=split_doc.page_content, metadata=split_doc.metadata)
            for split_doc in split_docs
        ]

    def _get_source(self) -> str:
        """Get the source identifier for this preprocessor"""
        return self.__class__.__name__

    def to_dataframe(self, chunks: List[ProcessedChunk]) -> pd.DataFrame:
        """Convert processed chunks to dataframe format"""
        return pd.DataFrame([chunk.model_dump() for chunk in chunks])

    @staticmethod
    def dict_to_document(data: Dict[str, Any]) -> Document:
        """Convert a dictionary to a Document object"""
        return Document(
            id=data.get("id"),
            content=data.get("content", ""),
            embeddings=data.get("embeddings"),
            metadata=data.get("metadata", {}),
        )

    def _generate_deterministic_id(
        self, content: str, content_column: str = None, provided_id: str = None
    ) -> str:
        """Generate a deterministic ID based on content and column"""
        if provided_id is not None:
            return f"{provided_id}_{content_column}"

        id_string = f"content={content}_column={content_column}"
        return hashlib.sha256(id_string.encode()).hexdigest()

    def _generate_chunk_id(
        self,
        content: str,
        chunk_index: Optional[int] = None,
        content_column: str = None,
        provided_id: str = None,
    ) -> str:
        """Generate deterministic ID for a chunk"""
        base_id = self._generate_deterministic_id(content, content_column, provided_id)
        chunk_id = (
            f"{base_id}_chunk_{chunk_index}" if chunk_index is not None else base_id
        )
        logger.debug(f"Generated chunk ID: {chunk_id} for content hash: {base_id}")
        return chunk_id

    def _prepare_chunk_metadata(
        self,
        doc_id: Optional[str],
        chunk_index: Optional[int],
        base_metadata: Optional[Dict] = None,
    ) -> Dict:
        """Centralized method for preparing chunk metadata"""
        metadata = base_metadata or {}

        # Always preserve original document ID
        if doc_id is not None:
            metadata["original_doc_id"] = doc_id

        # Add chunk index only for multi-chunk cases
        if chunk_index is not None:
            metadata["chunk_index"] = chunk_index

        # Always set source
        metadata["source"] = self._get_source()

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
        self.splitter = FileSplitter(
            FileSplitterConfig(
                chunk_size=config.chunk_size, chunk_overlap=config.chunk_overlap
            )
        )
        self.llm = create_chat_model(
            {
                "model_name": self.config.llm_config.model_name,
                "provider": self.config.llm_config.provider,
                **self.config.llm_config.params,
            }
        )
        self.context_template = config.context_template or self.DEFAULT_CONTEXT_TEMPLATE
        self.summarize = self.config.summarize

    def _prepare_prompts(
        self, chunk_contents: list[str], full_documents: list[str]
    ) -> list[str]:
        prompts = [
            self.context_template.replace("{{WHOLE_DOCUMENT}}", full_document)
            for full_document in full_documents
        ]
        prompts = [
            prompt.replace("{{CHUNK_CONTENT}}", chunk_content)
            for prompt, chunk_content in zip(prompts, chunk_contents)
        ]

        return prompts

    def _generate_context(
        self, chunk_contents: list[str], full_documents: list[str]
    ) -> list[str]:
        """Generate contextual description for a chunk using LLM"""
        prompts = self._prepare_prompts(chunk_contents, full_documents)

        # Check if LLM supports async
        if hasattr(self.llm, 'abatch'):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                response = loop.run_until_complete(self._async_generate(prompts))
            finally:
                loop.close()
        else:
            # Use sync batch for non-async LLMs
            response = [resp.content for resp in self.llm.batch(prompts)]
        return response

    async def _async_generate(self, prompts: list[str]) -> list[str]:
        """Helper for async LLM generation"""
        return [resp.content for resp in await self.llm.abatch(prompts)]

    def _split_document(self, doc: Document) -> List[Document]:
        """Split document into chunks while preserving metadata"""
        # Use base class implementation
        return super()._split_document(doc)

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        chunks_list = []
        doc_index_list = []
        chunk_index_list = []
        processed_chunks = []

        for doc_index, doc in enumerate(documents):
            # Get content_column from metadata if available
            content_column = (
                doc.metadata.get("content_column") if doc.metadata else None
            )

            # Ensure document has an ID
            if doc.id is None:
                doc.id = self._generate_deterministic_id(doc.content, content_column)

            # Skip empty or whitespace-only content
            if not doc.content or not doc.content.strip():
                continue

            chunk_docs = self._split_document(doc)

            # Single chunk case
            if len(chunk_docs) == 1:
                chunk_doc = chunk_docs[0]
                if not chunk_doc.content or not chunk_doc.content.strip():
                    continue
                else:
                    chunks_list.append(chunk_doc)
                    doc_index_list.append(doc_index)
                    chunk_index_list.append(0)

            else:
                # Multiple chunks case
                for i, chunk_doc in enumerate(chunk_docs):
                    if not chunk_doc.content or not chunk_doc.content.strip():
                        continue
                    else:
                        chunks_list.append(chunk_doc)
                        doc_index_list.append(doc_index)
                        chunk_index_list.append(i)

        # Generate contexts
        doc_contents = [documents[i].content for i in doc_index_list]
        chunk_contents = [chunk_doc.content for chunk_doc in chunks_list]
        contexts = self._generate_context(chunk_contents, doc_contents)

        for context, chunk_doc, chunk_index, doc_index in zip(
            contexts, chunks_list, chunk_index_list, doc_index_list
        ):
            processed_content = (
                context if self.summarize else f"{context}\n\n{chunk_doc.content}"
            )
            doc = documents[doc_index]

            # Initialize metadata
            metadata = {}
            if doc.metadata:
                metadata.update(doc.metadata)

            # Pass through doc.id and content_column
            content_column = (
                doc.metadata.get("content_column") if doc.metadata else None
            )
            chunk_id = self._generate_chunk_id(
                processed_content,
                chunk_index,
                content_column=content_column,
                provided_id=doc.id,
            )
            processed_chunks.append(
                ProcessedChunk(
                    id=chunk_id,
                    content=processed_content,  # Use the content with context
                    embeddings=doc.embeddings,
                    metadata=self._prepare_chunk_metadata(doc.id, None, metadata),
                )
            )

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
            separators=self.config.separators,
        )

    def _split_document(self, doc: Document) -> List[Document]:
        """Split document into chunks while preserving metadata"""
        # Use base class implementation
        return super()._split_document(doc)

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        processed_chunks = []

        for doc in documents:
            # Get content_column from metadata if available
            content_column = (
                doc.metadata.get("content_column") if doc.metadata else None
            )

            # Ensure document has an ID
            if doc.id is None:
                doc.id = self._generate_deterministic_id(doc.content, content_column)

            # Skip empty or whitespace-only content
            if not doc.content or not doc.content.strip():
                continue

            chunk_docs = self._split_document(doc)

            # Single chunk case
            if len(chunk_docs) == 1:
                chunk_doc = chunk_docs[0]
                if not chunk_doc.content or not chunk_doc.content.strip():
                    continue

                # Initialize metadata
                metadata = {}
                if doc.metadata:
                    metadata.update(doc.metadata)

                # Pass through doc.id and content_column
                id = self._generate_chunk_id(
                    chunk_doc.content, content_column=content_column, provided_id=doc.id
                )
                processed_chunks.append(
                    ProcessedChunk(
                        id=id,
                        content=chunk_doc.content,
                        embeddings=doc.embeddings,
                        metadata=self._prepare_chunk_metadata(doc.id, None, metadata),
                    )
                )
            else:
                # Multiple chunks case
                for i, chunk_doc in enumerate(chunk_docs):
                    if not chunk_doc.content or not chunk_doc.content.strip():
                        continue

                    # Initialize metadata
                    metadata = {}
                    if doc.metadata:
                        metadata.update(doc.metadata)

                    # Pass through doc.id and content_column
                    chunk_id = self._generate_chunk_id(
                        chunk_doc.content,
                        i,
                        content_column=content_column,
                        provided_id=doc.id,
                    )
                    processed_chunks.append(
                        ProcessedChunk(
                            id=chunk_id,
                            content=chunk_doc.content,
                            embeddings=doc.embeddings,
                            metadata=self._prepare_chunk_metadata(doc.id, i, metadata),
                        )
                    )

        return processed_chunks


class PreprocessorFactory:
    """Factory for creating preprocessors based on configuration"""

    @staticmethod
    def create_preprocessor(
        config: Optional[PreprocessingConfig] = None,
    ) -> DocumentPreprocessor:
        """
        Create appropriate preprocessor based on configuration
        : param config: Preprocessing configuration
        : return: Configured preprocessor instance
        : raises ValueError: If unknown preprocessor type specified
        """
        if config is None:
            return TextChunkingPreprocessor()

        if config.type == PreprocessorType.CONTEXTUAL:
            return ContextualPreprocessor(
                config.contextual_config or ContextualConfig()
            )
        elif config.type == PreprocessorType.TEXT_CHUNKING:
            return TextChunkingPreprocessor(config.text_chunking_config)

        raise ValueError(f"Unknown preprocessor type: {config.type}")
