import os
from typing import List, Iterator
from langchain_core.documents import Document as LangchainDocument
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
import pandas as pd

from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.utilities.rag.loaders.file_loader import FileLoader
from mindsdb.integrations.utilities.rag.splitters.file_splitter import (
    FileSplitter,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CHUNK_OVERLAP
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
            mysql_proxy=None
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

        # Initialize text splitter for query results with default settings
        self.query_splitter = RecursiveCharacterTextSplitter(
            chunk_size=DEFAULT_CHUNK_SIZE,
            chunk_overlap=DEFAULT_CHUNK_OVERLAP
        )

    def load_files(self, file_names: List[str]) -> Iterator[Document]:
        """Load and split documents from files"""
        for file_name in file_names:
            file_path = self.file_controller.get_file_path(file_name)
            loader = self.file_loader_class(file_path)

            for doc in loader.lazy_load():
                # Add file extension to metadata for proper splitting
                extension = os.path.splitext(file_path)[1].lower()
                doc.metadata['extension'] = extension
                doc.metadata['source'] = file_name

                # Use FileSplitter to handle the document based on its type
                split_docs = self.file_splitter.split_documents([doc])
                for split_doc in split_docs:
                    # Preserve original metadata while adding split-specific metadata
                    metadata = doc.metadata.copy()
                    metadata.update(split_doc.metadata or {})

                    yield Document(
                        content=split_doc.page_content,
                        metadata=metadata
                    )

    def load_web_pages(
            self,
            urls: List[str],
            crawl_depth: int,
            limit: int,
            filters: List[str] = None,
    ) -> Iterator[Document]:
        """Load and split documents from web pages"""
        websites_df = get_all_websites(
            urls,
            crawl_depth=crawl_depth,
            limit=limit,
            filters=filters
        )

        for _, row in websites_df.iterrows():
            # Create a document with HTML extension for proper splitting
            doc = LangchainDocument(
                page_content=row['text_content'],
                metadata={
                    'extension': '.html',
                    'url': row['url']
                }
            )

            # Use FileSplitter to handle HTML content
            split_docs = self.file_splitter.split_documents([doc])
            for split_doc in split_docs:
                metadata = doc.metadata.copy()
                metadata.update(split_doc.metadata or {})

                yield Document(
                    content=split_doc.page_content,
                    metadata=metadata
                )

    def load_query_result(self, query: str, project_name: str) -> Iterator[Document]:
        """
        Load documents from SQL query results

        Args:
            query: SQL query to execute
            project_name: Name of the project context

        Returns:
            Iterator of Document objects

        Raises:
            ValueError: If mysql_proxy is not configured or query returns no data
        """
        if not self.mysql_proxy:
            raise ValueError("MySQL proxy not configured")

        if not query:
            return

        # Set project context and execute query
        self.mysql_proxy.set_context({'db': project_name})
        query_result = self.mysql_proxy.process_query(query)

        if query_result.type != 'table':
            raise ValueError('Query returned no data')

        # Convert query result to DataFrame
        df = query_result.data.to_df()

        # Process each row into a Document
        for _, row in df.iterrows():
            # Extract content and metadata
            content = str(row.get('content', ''))

            # Convert remaining columns to metadata
            metadata = {
                col: str(row[col])
                for col in df.columns
                if col != 'content' and not pd.isna(row[col])
            }
            metadata['source'] = 'query'

            # Split content using recursive splitter
            if content:
                doc = LangchainDocument(
                    page_content=content,
                    metadata=metadata
                )
                # Use FileSplitter with default recursive splitter
                split_docs = self.file_splitter.split_documents(
                    [doc],
                    default_failover=True
                )

                for split_doc in split_docs:
                    metadata = doc.metadata.copy()
                    metadata.update(split_doc.metadata or {})

                    yield Document(
                        content=split_doc.page_content,
                        metadata=metadata
                    )
