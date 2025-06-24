import os
from typing import List, Iterator
from langchain_core.documents import Document as LangchainDocument
from langchain_text_splitters import MarkdownHeaderTextSplitter

from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.utilities.rag.loaders.file_loader import FileLoader
from mindsdb.integrations.utilities.rag.splitters.file_splitter import (
    FileSplitter,
)
from mindsdb.integrations.handlers.web_handler.urlcrawl_helpers import get_all_websites
from mindsdb.interfaces.knowledge_base.preprocessing.models import Document
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class DocumentLoader:
    """Handles loading documents from various sources including SQL queries"""

    def __init__(
        self,
        file_controller: FileController,
        file_splitter: FileSplitter,
        markdown_splitter: MarkdownHeaderTextSplitter,
        file_loader_class=FileLoader,
        mysql_proxy=None,
    ):
        """
        Initialize with required dependencies

        Args:
            file_controller: Controller for file operations
            file_splitter: Splitter for file content
            markdown_splitter: Splitter for markdown content
            file_loader_class: Class to use for file loading
            mysql_proxy: Proxy for executing MySQL queries
        """
        self.file_controller = file_controller
        self.file_splitter = file_splitter
        self.markdown_splitter = markdown_splitter
        self.file_loader_class = file_loader_class
        self.mysql_proxy = mysql_proxy

    def load_files(self, file_names: List[str]) -> Iterator[Document]:
        """Load and split documents from files"""
        for file_name in file_names:
            file_path = self.file_controller.get_file_path(file_name)
            loader = self.file_loader_class(file_path)

            for doc in loader.lazy_load():
                # Add file extension to metadata for proper splitting
                extension = os.path.splitext(file_path)[1].lower()
                doc.metadata["extension"] = extension
                doc.metadata["source"] = file_name

                # Use FileSplitter to handle the document based on its type
                split_docs = self.file_splitter.split_documents([doc])
                for split_doc in split_docs:
                    # Preserve original metadata while adding split-specific metadata
                    metadata = doc.metadata.copy()
                    metadata.update(split_doc.metadata or {})

                    yield Document(content=split_doc.page_content, metadata=metadata)

    def load_web_pages(
        self,
        urls: List[str],
        crawl_depth: int,
        limit: int,
        filters: List[str] = None,
    ) -> Iterator[Document]:
        """Load and split documents from web pages"""
        websites_df = get_all_websites(urls, crawl_depth=crawl_depth, limit=limit, filters=filters)

        for _, row in websites_df.iterrows():
            # Create a document with HTML extension for proper splitting
            doc = LangchainDocument(
                page_content=row["text_content"], metadata={"extension": ".html", "url": row["url"]}
            )

            # Use FileSplitter to handle HTML content
            split_docs = self.file_splitter.split_documents([doc])
            for split_doc in split_docs:
                metadata = doc.metadata.copy()
                metadata.update(split_doc.metadata or {})

                yield Document(content=split_doc.page_content, metadata=metadata)
