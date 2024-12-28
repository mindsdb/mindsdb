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


@dataclass
class FileSplitterConfig:
    """Represents configuration needed to split a file into chunks for retrieval."""

    # Target chunk size in characters. Not all splitters will adhere exactly to this (it's more of a guideline)
    chunk_size: int = DEFAULT_CHUNK_SIZE
    # How many characters each chunk should overlap. Not all splitters will adhere exactly to this (it's more of a guideline)
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP
    # Chunking parameters are passed as a TextChunkingConfig
    text_chunking_config: TextChunkingConfig = None
    # Default recursive splitter to use for text files, or unsupported files
    recursive_splitter: RecursiveCharacterTextSplitter = None
    # Splitter to use for MD splitting
    markdown_splitter: MarkdownHeaderTextSplitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=DEFAULT_MARKDOWN_HEADERS_TO_SPLIT_ON
    )
    # Splitter to use for HTML splitting
    html_splitter: HTMLHeaderTextSplitter = HTMLHeaderTextSplitter(
        headers_to_split_on=DEFAULT_HTML_HEADERS_TO_SPLIT_ON
    )

    def __post_init__(self):
        if self.text_chunking_config is None:
            self.text_chunking_config = TextChunkingConfig(
                chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap
            )

        if self.recursive_splitter is None:
            self.recursive_splitter = RecursiveCharacterTextSplitter(
                chunk_size=self.text_chunking_config.chunk_size,
                chunk_overlap=self.text_chunking_config.chunk_overlap,
                length_function=self.text_chunking_config.length_function,
                separators=self.text_chunking_config.separators,
            )


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

    def split_documents(
        self, documents: List[Document], default_failover: bool = True
    ) -> List[Document]:
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
            split_func = self._split_func_by_extension(extension=extension)
            try:
                split_documents += split_func(document.page_content)
            except Exception as e:
                logger.error(
                    f"Error splitting document with extension {extension}: {str(e)}"
                )
                if not default_failover:
                    raise ValueError(
                        f"Error splitting document with extension {extension}"
                    ) from e
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
