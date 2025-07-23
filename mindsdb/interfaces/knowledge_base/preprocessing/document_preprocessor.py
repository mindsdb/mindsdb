import re
import html
import asyncio
from typing import List, Dict, Optional, Any

import pandas as pd
from mindsdb.interfaces.knowledge_base.preprocessing.text_splitter import TextSplitter

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

logger = log.getLogger(__name__)

_DEFAULT_CONTENT_COLUMN_NAME = "content"


class DocumentPreprocessor:
    """Base class for document preprocessing"""

    def __init__(self):
        """Initialize preprocessor"""
        self.splitter = None  # Will be set by child classes
        self.config = None

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        """Base implementation - should be overridden by child classes

        Args:
            documents: List of documents to process
        """
        raise NotImplementedError("Subclasses must implement process_documents")

    def _split_document(self, doc: Document) -> List[Document]:
        """Split document into chunks while preserving metadata"""
        if self.splitter is None:
            raise ValueError("Splitter not configured")

        metadata = doc.metadata or {}
        # Split and convert back to our Document type
        split_texts = self.splitter.split_text(doc.content)
        return [Document(content=text, metadata=metadata) for text in split_texts]

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

    def _generate_chunk_id(
        self,
        chunk_index: Optional[int] = None,
        total_chunks: Optional[int] = None,
        start_char: Optional[int] = None,
        end_char: Optional[int] = None,
        provided_id: str = None,
        content_column: str = None,
    ) -> str:
        """Generate human-readable deterministic ID for a chunk
        Format: <doc_id>:<content_column>:<chunk_number>of<total_chunks>:<start_char>to<end_char>
        """
        if provided_id is None:
            raise ValueError("Document ID must be provided for chunk ID generation")

        if content_column is None:
            raise ValueError("Content column must be provided for chunk ID generation")

        chunk_id = f"{provided_id}:{content_column}:{chunk_index + 1}of{total_chunks}:{start_char}to{end_char}"
        logger.debug(f"Generated chunk ID: {chunk_id}")
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
            metadata[self.config.doc_id_column_name] = doc_id

        # Add chunk index only for multi-chunk cases
        if chunk_index is not None:
            metadata["_chunk_index"] = chunk_index

        # Always set source
        metadata["_source"] = self._get_source()

        return metadata


class ContextualPreprocessor(DocumentPreprocessor):
    """Contextual preprocessing implementation that enhances document chunks with context"""

    DEFAULT_CONTEXT_TEMPLATE = """
<document>
{WHOLE_DOCUMENT}
</document>
Here is the chunk we want to situate within the whole document
<chunk>
{CHUNK_CONTENT}
</chunk>
Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk. Answer only with the succinct context and nothing else."""

    def __init__(self, config: ContextualConfig):
        """Initialize with contextual configuration"""
        super().__init__()
        self.config = config
        self.splitter = FileSplitter(
            FileSplitterConfig(chunk_size=config.chunk_size, chunk_overlap=config.chunk_overlap)
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

    def _prepare_prompts(self, chunk_contents: list[str], full_documents: list[str]) -> list[str]:
        def tag_replacer(match):
            tag = match.group(0)
            if tag.lower() not in ["<document>", "</document>", "<chunk>", "</chunk>"]:
                return tag
            return html.escape(tag)

        tag_pattern = r"</?document>|</?chunk>"
        prompts = []
        for chunk_content, full_document in zip(chunk_contents, full_documents):
            chunk_content = re.sub(tag_pattern, tag_replacer, chunk_content, flags=re.IGNORECASE)
            full_document = re.sub(tag_pattern, tag_replacer, full_document, flags=re.IGNORECASE)
            prompts.append(
                self.DEFAULT_CONTEXT_TEMPLATE.format(WHOLE_DOCUMENT=full_document, CHUNK_CONTENT=chunk_content)
            )

        return prompts

    def _generate_context(self, chunk_contents: list[str], full_documents: list[str]) -> list[str]:
        """Generate contextual description for a chunk using LLM"""
        prompts = self._prepare_prompts(chunk_contents, full_documents)

        # Check if LLM supports async
        if hasattr(self.llm, "abatch"):
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
            # Document ID must be provided by this point
            if doc.id is None:
                raise ValueError("Document ID must be provided before preprocessing")

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

        for context, chunk_doc, chunk_index, doc_index in zip(contexts, chunks_list, chunk_index_list, doc_index_list):
            processed_content = context if self.summarize else f"{context}\n\n{chunk_doc.content}"
            doc = documents[doc_index]

            # Initialize metadata
            metadata = {}
            if doc.metadata:
                metadata.update(doc.metadata)

            # Get content_column from metadata or use default
            content_column = metadata.get("_content_column")
            if content_column is None:
                # If content_column is not in metadata, use the default column name
                content_column = _DEFAULT_CONTENT_COLUMN_NAME
                logger.debug(f"No content_column found in metadata, using default: {_DEFAULT_CONTENT_COLUMN_NAME}")

            chunk_id = self._generate_chunk_id(
                chunk_index=chunk_index, provided_id=doc.id, content_column=content_column
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
    """Default text chunking preprocessor using TextSplitter"""

    def __init__(self, config: Optional[TextChunkingConfig] = None):
        """Initialize with text chunking configuration"""
        super().__init__()
        self.config = config or TextChunkingConfig()
        self.splitter = TextSplitter(
            chunk_size=self.config.chunk_size,
            chunk_overlap=self.config.chunk_overlap,
            separators=self.config.separators,
        )

    def _split_document(self, doc: Document) -> List[Document]:
        """Split document into chunks while preserving metadata"""
        # Use base class implementation
        return super()._split_document(doc)

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        processed_chunks = []

        for doc in documents:
            # Document ID must be provided by this point
            if doc.id is None:
                raise ValueError("Document ID must be provided before preprocessing")

            # Skip empty or whitespace-only content
            if not doc.content or not doc.content.strip():
                continue

            chunk_docs = self._split_document(doc)
            total_chunks = len(chunk_docs)

            # Track character positions
            current_pos = 0
            for i, chunk_doc in enumerate(chunk_docs):
                if not chunk_doc.content or not chunk_doc.content.strip():
                    continue

                # Calculate chunk positions
                start_char = current_pos
                end_char = start_char + len(chunk_doc.content)
                current_pos = end_char + 1  # +1 for separator

                # Initialize metadata
                metadata = {}
                if doc.metadata:
                    metadata.update(doc.metadata)

                # Add position metadata
                metadata["_start_char"] = start_char
                metadata["_end_char"] = end_char

                # Get content_column from metadata or use default
                content_column = None
                if doc.metadata:
                    content_column = doc.metadata.get("_content_column")

                if content_column is None:
                    # If content_column is not in metadata, use the default column name
                    content_column = _DEFAULT_CONTENT_COLUMN_NAME
                    logger.debug(f"No content_column found in metadata, using default: {_DEFAULT_CONTENT_COLUMN_NAME}")

                chunk_id = self._generate_chunk_id(
                    chunk_index=i,
                    total_chunks=total_chunks,
                    start_char=start_char,
                    end_char=end_char,
                    provided_id=doc.id,
                    content_column=content_column,
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
        :param config: Preprocessing configuration
        :return: Configured preprocessor instance
        :raises ValueError: If unknown preprocessor type specified
        """
        if config is None:
            # Default to text chunking if no config provided
            return TextChunkingPreprocessor()

        if config.type == PreprocessorType.TEXT_CHUNKING:
            return TextChunkingPreprocessor(config.text_chunking_config)
        elif config.type == PreprocessorType.CONTEXTUAL:
            return ContextualPreprocessor(config.contextual_config)
        elif config.type == PreprocessorType.JSON_CHUNKING:
            # Import here to avoid circular imports
            from mindsdb.interfaces.knowledge_base.preprocessing.json_chunker import JSONChunkingPreprocessor

            return JSONChunkingPreprocessor(config.json_chunking_config)
        else:
            raise ValueError(f"Unknown preprocessor type: {config.type}")
