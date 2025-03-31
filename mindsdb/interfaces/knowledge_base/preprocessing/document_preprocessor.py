from typing import List, Dict, Optional, Any, Union
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

    def process_documents(self, documents: List[Document]) -> tuple[List[ProcessedChunk], List[Union[str, int]]]:
        """Base implementation - should be overridden by child classes

        Returns:
            tuple: (processed_chunks, failed_doc_ids)
                - processed_chunks: List of successfully processed chunks
                - failed_doc_ids: List of document IDs that failed processing
        """
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
        base_id = provided_id
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

    def process_documents(self, documents: List[Document]) -> tuple[List[ProcessedChunk], List[str]]:
        chunks_list = []
        doc_index_list = []
        chunk_index_list = []
        processed_chunks = []
        failed_doc_ids = []  # Store IDs of failed documents

        for doc_index, doc in enumerate(documents):
            # Get content_column from metadata if available
            content_column = (
                doc.metadata.get("content_column") if doc.metadata else None
            )

            # Ensure document has an ID (should be provided by caller)
            doc_id = doc.id
            if doc_id is None:
                logger.error(f"Document at index {doc_index} received without an ID during preprocessing. Skipping.")
                failed_doc_ids.append(f"document_{doc_index+1}_missing_id")
                continue

            # Skip empty or whitespace-only content
            if not doc.content or not doc.content.strip():
                logger.warning(f"Document {doc_id} has empty or whitespace-only content. Skipping.")
                continue

            try:
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
            except Exception as e:
                logger.error(f"Error processing document {doc_id}: {str(e)}")
                failed_doc_ids.append(doc_id)
                continue

        # Generate contexts
        try:
            doc_contents = [documents[i].content for i in doc_index_list]
            chunk_contents = [chunk_doc.content for chunk_doc in chunks_list]
            contexts = self._generate_context(chunk_contents, doc_contents)
        except Exception as e:
            logger.error(f"Error generating contexts: {str(e)}")
            # If context generation fails, we can't process any chunks
            return [], [doc.id for doc in documents if doc.id is not None]

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

        logger.info(f"Contextual preprocessing complete. Created {len(processed_chunks)} chunks. Failed documents: {len(failed_doc_ids)}")
        return processed_chunks, failed_doc_ids


class TextChunkingPreprocessor(DocumentPreprocessor):
    """Default text chunking preprocessor using RecursiveCharacterTextSplitter with token-based chunking"""

    def __init__(self, config: Optional[TextChunkingConfig] = None):
        """Initialize with text chunking configuration"""
        super().__init__()
        self.config = config or TextChunkingConfig()

        # Use TokenTextSplitter with tiktoken encoder
        self.splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            encoding_name=self.config.encoding_name,
            chunk_size=self.config.chunk_size,
            chunk_overlap=self.config.chunk_overlap,
            disallowed_special=(),  # Allow all special tokens
            separators=self.config.separators
        )

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

    def process_documents(self, documents: List[Document]) -> tuple[List[ProcessedChunk], List[Union[str, int]]]:
        """Process documents into chunks with metadata"""
        logger.info(f"Starting document preprocessing for {len(documents)} documents")
        processed_chunks = []
        failed_doc_ids = []  # Store IDs of failed documents
        total_chars = sum(len(doc.content) for doc in documents)
        logger.info(f"Total content size: {total_chars} characters")

        failed_docs_count = 0
        for i, doc in enumerate(documents):
            # Assume doc.id is always provided by the caller (e.g., KnowledgeBaseTable.insert)
            doc_id = doc.id
            if doc_id is None:
                # This case should ideally not happen if called from KnowledgeBaseTable.insert
                logger.error(f"Document at index {i} received without an ID during preprocessing. Skipping.")
                failed_docs_count += 1
                # Use a placeholder for failed_doc_ids list if ID is missing
                failed_doc_ids.append(f"document_{i+1}_missing_id")
                continue

            try:
                # Split document into chunks
                chunks = self._split_document(doc)
                logger.debug(f"Document {doc_id} split into {len(chunks)} chunks")

                for chunk_idx, chunk in enumerate(chunks):
                    # Generate chunk ID based on the existing doc.id
                    chunk_id = self._generate_chunk_id(
                        chunk.content,
                        chunk_idx,
                        provided_id=doc_id
                    )

                    # Prepare metadata
                    metadata = chunk.metadata or {}
                    metadata.update({
                        "original_doc_id": doc_id,
                        "chunk_index": chunk_idx,
                        "total_chunks": len(chunks)
                    })

                    processed_chunks.append(
                        ProcessedChunk(
                            id=chunk_id,
                            content=chunk.content,
                            metadata=metadata
                        )
                    )
            except Exception as e:
                failed_docs_count += 1
                logger.error(f"Error processing document {doc_id}: {str(e)}")
                failed_doc_ids.append(doc_id)  # Add failed ID to the list
                # Continue to the next document on error
                continue

        processed_doc_count = len(documents) - failed_docs_count

        # Log a warning if any documents failed
        if failed_docs_count > 0:
            warning_message = f"Failed to process {failed_docs_count} out of {len(documents)} documents. Failed IDs: {failed_doc_ids}"
            logger.warning(warning_message)

        # Check if any chunks were produced from the successfully processed documents
        if processed_doc_count > 0 and not processed_chunks:
            logger.warning(f"Successfully processed {processed_doc_count} documents, but no chunks were generated. Check document content and chunking configuration.")
        elif processed_doc_count == 0 and not processed_chunks:
            # Only raise error if NO documents were processed successfully AND no chunks were created.
            # Avoids raising error if input was empty list.
            if len(documents) > 0:
                raise RuntimeError(f"Failed to process all {len(documents)} documents. No chunks were created. Check logs and input data.")
            else:
                logger.info("Preprocessing received an empty list of documents.")

        logger.info(f"Preprocessing complete. Created {len(processed_chunks)} chunks from {processed_doc_count} successfully processed documents.")
        if processed_doc_count > 0:
            logger.info(f"Average chunks per successfully processed document: {len(processed_chunks)/processed_doc_count:.2f}")

        # Return both successful chunks and the list of failed document IDs
        return processed_chunks, failed_doc_ids


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
