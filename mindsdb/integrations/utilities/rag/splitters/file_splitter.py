from dataclasses import dataclass
from typing import Callable, List, Union

from langchain_core.documents import Document
from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)

from mindsdb.interfaces.knowledge_base.preprocessing.models import TextChunkingConfig

from mindsdb.utilities import log

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 50
DEFAULT_MARKDOWN_HEADERS_TO_SPLIT_ON = [
    ("#", "Header 1"),
    ("##", "Header 2"),
    ("###", "Header 3"),
]
DEFAULT_HTML_HEADERS_TO_SPLIT_ON = [
    ("h1", "Header 1"),
    ("h2", "Header 2"),
    ("h3", "Header 3"),
    ("h4", "Header 4"),
]
logger = log.getLogger(__name__)


class PDFPageSplitter:
    """Splits PDF documents by pages, preserving page metadata."""

    def __init__(self, max_page_size: int = 4000, chunk_overlap: int = 200):
        """
        Initialize PDF page splitter.

        Args:
            max_page_size: Maximum characters per page chunk. If a page exceeds this,
                          it will be split using recursive character splitting.
            chunk_overlap: Overlap between chunks when splitting large pages.
        """
        self.max_page_size = max_page_size
        self.chunk_overlap = chunk_overlap
        self.recursive_splitter = RecursiveCharacterTextSplitter(
            chunk_size=max_page_size, chunk_overlap=chunk_overlap, separators=["\n\n", "\n", ". ", " ", ""]
        )

    def split_pdf_by_pages(self, pdf_content: bytes, source_name: str = None) -> List[Document]:
        """
        Split PDF content by pages, with each page as a separate chunk.

        Args:
            pdf_content: Raw PDF bytes
            source_name: Name of the source file

        Returns:
            List of Document objects, one per page
        """
        try:
            import fitz  # PyMuPDF
        except ImportError:
            logger.error("PyMuPDF (fitz) is required for PDF page splitting. Install with: pip install PyMuPDF")
            raise ImportError("PyMuPDF is required for PDF page splitting")

        documents = []

        try:
            with fitz.open(stream=pdf_content, filetype="pdf") as pdf_doc:
                total_pages = len(pdf_doc)

                for page_num in range(total_pages):
                    page = pdf_doc.load_page(page_num)
                    page_text = page.get_text()

                    if not page_text.strip():
                        continue

                    # Create metadata with page information
                    metadata = {
                        "source": source_name or "pdf_document",
                        "page_number": page_num + 1,  # 1-indexed for user convenience
                        "total_pages": total_pages,
                        "file_type": "pdf",
                    }

                    # If page is too large, split it further
                    if len(page_text) > self.max_page_size:
                        logger.debug(
                            f"Page {page_num + 1} exceeds max size ({len(page_text)} > {self.max_page_size}), splitting further"
                        )
                        page_chunks = self.recursive_splitter.split_text(page_text)

                        for chunk_idx, chunk_text in enumerate(page_chunks):
                            chunk_metadata = metadata.copy()
                            chunk_metadata["chunk_index"] = chunk_idx
                            chunk_metadata["total_chunks"] = len(page_chunks)

                            documents.append(Document(page_content=chunk_text, metadata=chunk_metadata))
                    else:
                        # Page fits within size limit, use as single chunk
                        documents.append(Document(page_content=page_text, metadata=metadata))

        except Exception as e:
            logger.error(f"Error processing PDF: {str(e)}")
            raise ValueError(f"Failed to process PDF: {str(e)}")

        logger.info(f"Split PDF into {len(documents)} chunks across {total_pages} pages")
        return documents


@dataclass
class FileSplitterConfig:
    """Represents configuration needed to split a file into chunks for retrieval."""

    # Target chunk size in characters. Not all splitters will adhere exactly to this (it's more of a guideline)
    chunk_size: int = DEFAULT_CHUNK_SIZE
    # How many characters each chunk should overlap. Not all splitters will adhere exactly to this (it's more of a guideline)
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP
    # Whether to use page-based chunking for PDFs instead of character-based chunking
    page_chunking: bool = False
    # Maximum size for individual pages when using page chunking (if page exceeds this, it will be split further)
    max_page_size: int = 4000
    # Chunking parameters are passed as a TextChunkingConfig
    text_chunking_config: TextChunkingConfig = None
    # Default recursive splitter to use for text files, or unsupported files
    recursive_splitter: RecursiveCharacterTextSplitter = None
    # Splitter to use for MD splitting
    markdown_splitter: MarkdownHeaderTextSplitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=DEFAULT_MARKDOWN_HEADERS_TO_SPLIT_ON
    )
    # Splitter to use for HTML splitting
    html_splitter: HTMLHeaderTextSplitter = HTMLHeaderTextSplitter(headers_to_split_on=DEFAULT_HTML_HEADERS_TO_SPLIT_ON)
    # PDF page splitter for page-based chunking
    pdf_page_splitter: PDFPageSplitter = None

    def __post_init__(self):
        if self.text_chunking_config is None:
            self.text_chunking_config = TextChunkingConfig(chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap)

        if self.recursive_splitter is None:
            self.recursive_splitter = RecursiveCharacterTextSplitter(
                chunk_size=self.text_chunking_config.chunk_size,
                chunk_overlap=self.text_chunking_config.chunk_overlap,
                length_function=self.text_chunking_config.length_function,
                separators=self.text_chunking_config.separators,
            )

        if self.pdf_page_splitter is None:
            self.pdf_page_splitter = PDFPageSplitter(max_page_size=self.max_page_size, chunk_overlap=self.chunk_overlap)


class FileSplitter:
    """Splits Documents that represent various file types into chunks for retrieval."""

    def __init__(self, config: FileSplitterConfig):
        """
        Args:
            config (FileSplitterConfig): Configuration for the file splitter.
        """
        self.config = config
        self._extension_map = {
            ".pdf": self._recursive_splitter_fn,
            ".md": self._markdown_splitter_fn,
            ".html": self._html_splitter_fn,
        }
        self.default_splitter = self._recursive_splitter_fn

    def _split_func_by_extension(
        self, extension
    ) -> Union[Callable, HTMLHeaderTextSplitter, MarkdownHeaderTextSplitter]:
        return self._extension_map.get(extension, self.default_splitter)()

    def split_documents(self, documents: List[Document], default_failover: bool = True) -> List[Document]:
        """Splits a list of documents representing files using the appropriate splitting & chunking strategies

        Args:
            documents (List[Document]): List of documents representing files to split.
            default_failover (bool, optional): Whether to use the default splitter as a fallback if the file type is not supported. Defaults to True.

        Returns:
            List[Document]: List of documents representing the split files.
        """
        split_documents = []
        document: Document
        for document in documents:
            extension = document.metadata.get("extension")

            # Special handling for PDFs with page chunking enabled
            if extension == ".pdf" and self.config.page_chunking:
                try:
                    # Check if we have raw PDF content in metadata
                    pdf_content = document.metadata.get("pdf_content")
                    if pdf_content:
                        # Use page-based splitting
                        page_docs = self.config.pdf_page_splitter.split_pdf_by_pages(
                            pdf_content, source_name=document.metadata.get("source")
                        )
                        split_documents += page_docs
                        continue
                    else:
                        logger.warning(
                            "PDF page chunking enabled but no raw PDF content found in metadata. Falling back to text-based splitting."
                        )
                except Exception as e:
                    logger.error(f"Error in PDF page splitting: {str(e)}. Falling back to text-based splitting.")

            # Use regular splitting for all other cases
            split_func = self._split_func_by_extension(extension=extension)
            try:
                split_documents += split_func(document.page_content)
            except Exception as e:
                logger.error(f"Error splitting document with extension {extension}: {str(e)}")
                if not default_failover:
                    raise ValueError(f"Error splitting document with extension {extension}") from e
                # Try default splitter as a failover, if enabled.
                split_func = self._split_func_by_extension(extension=None)
                split_documents += split_func(document.page_content)
        return split_documents

    def _markdown_splitter_fn(self) -> Callable:
        return self.config.markdown_splitter.split_text

    def _html_splitter_fn(self) -> Callable:
        return self.config.html_splitter.split_text

    def _recursive_splitter_fn(self) -> Callable:
        # Recursive splitter is a TextSplitter where split_text returns List[str].
        def recursive_split(content: str) -> List[Document]:
            split_content = self.config.recursive_splitter.split_text(content)
            return [Document(page_content=c) for c in split_content]

        return recursive_split
