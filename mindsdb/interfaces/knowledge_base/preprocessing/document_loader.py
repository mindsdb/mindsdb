from typing import List, Iterator
from langchain_text_splitters import MarkdownHeaderTextSplitter

from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.utilities.rag.loaders.file_loader import FileLoader
from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter
from mindsdb.integrations.handlers.web_handler.urlcrawl_helpers import get_all_websites
from mindsdb.interfaces.knowledge_base.preprocessing.models import Document
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class DocumentLoader:
    """Handles loading documents from various sources"""

    def __init__(
            self,
            file_controller: FileController,
            file_splitter: FileSplitter,
            markdown_splitter: MarkdownHeaderTextSplitter,
            file_loader_class=FileLoader
    ):
        """Initialize with required dependencies"""
        self.file_controller = file_controller
        self.file_splitter = file_splitter
        self.markdown_splitter = markdown_splitter
        self.file_loader_class = file_loader_class

    def load_files(self, file_names: List[str]) -> Iterator[Document]:
        """Load and split documents from files"""
        for file_name in file_names:
            file_path = self.file_controller.get_file_path(file_name)
            loader = self.file_loader_class(file_path)

            for doc in loader.lazy_load():
                split_docs = self.file_splitter.split_documents([doc])
                for split_doc in split_docs:
                    yield Document(
                        content=split_doc.page_content,
                        metadata={'source': file_name}
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
            docs = self.markdown_splitter.split_text(row['text_content'])
            for doc in docs:
                yield Document(
                    content=doc.page_content,
                    metadata={'url': row['url']}
                )
