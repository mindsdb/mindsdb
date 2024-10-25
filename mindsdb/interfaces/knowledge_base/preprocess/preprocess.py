from typing import List, Dict, Optional
import pandas as pd
from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseTable
from mindsdb.integrations.utilities.rag.loaders.file_loader import FileLoader
from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.handlers.web_handler.urlcrawl_helpers import get_all_websites
from mindsdb.interfaces.knowledge_base.preprocess.models import (
    PreprocessingConfig,
    PreprocessorFactory,
    ProcessedChunk
)
from mindsdb.utilities import log
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.api.executor.exceptions import ExecutorException


_DEFAULT_MARKDOWN_HEADERS_TO_SPLIT_ON = [
    ("#", "Header 1"),
    ("##", "Header 2"),
    ("###", "Header 3"),
]

_DEFAULT_WEB_CRAWL_LIMIT = 100

logger = log.getLogger(__name__)


class PreprocessingKnowledgeBase:
    """Handles preprocessing for different types of inputs before inserting into knowledge base"""
    def __init__(self, table: KnowledgeBaseTable, preprocessing_config: Optional[PreprocessingConfig] = None):
        self.table = table
        self.preprocessing_config = preprocessing_config
        self.preprocessor = PreprocessorFactory.create_preprocessor(preprocessing_config) if preprocessing_config else None

    def _preprocess_documents(self, documents: List[Dict]) -> List[ProcessedChunk]:
        """Preprocess documents if preprocessor is configured"""
        if self.preprocessor:
            return self.preprocessor.process_documents(documents)
        return [ProcessedChunk(**doc) for doc in documents]

    def _chunks_to_dataframe(self, chunks: List[ProcessedChunk]) -> pd.DataFrame:
        """Convert processed chunks to dataframe format for KB insertion"""
        return pd.DataFrame([chunk.model_dump() for chunk in chunks])

    def process_and_insert_query_result(self, query: str, project_name: str):
        """Process and insert SQL query results with preprocessing"""
        if not query:
            return

        mysql_proxy = FakeMysqlProxy()
        # Use same project as API request for SQL context
        mysql_proxy.set_context({'db': project_name})
        query_result = mysql_proxy.process_query(query)

        if query_result.type != SQL_RESPONSE_TYPE.TABLE:
            raise ExecutorException('Query returned no data')

        # Extract column names, handling aliases
        column_names = [c.get('alias', c.get('name')) for c in query_result.columns]
        df = pd.DataFrame.from_records(query_result.data, columns=column_names)

        # Insert directly without document conversion
        self.table.insert(df)

    def process_and_insert_files(self, file_names: List[str]):
        """Process and insert files with preprocessing"""
        file_controller = FileController()
        splitter = FileSplitter(FileSplitterConfig())

        for file_name in file_names:
            file_path = file_controller.get_file_path(file_name)
            loader = FileLoader(file_path)

            # Collect all documents for batch preprocessing
            raw_documents = []
            for doc in loader.lazy_load():
                split_docs = splitter.split_documents([doc])
                for split_doc in split_docs:
                    raw_documents.append({
                        'content': split_doc.page_content,
                        'metadata': {'source': file_name}
                    })

            # Preprocess and insert
            if raw_documents:
                processed_chunks = self._preprocess_documents(raw_documents)
                df = self._chunks_to_dataframe(processed_chunks)
                self.table.insert(df)

    def process_and_insert_web_pages(
            self,
            urls: List[str],
            crawl_depth: int = 1,
            filters: List[str] = None,
            default_headers: List[tuple] = None
    ):
        """Process and insert web pages with preprocessing"""

        try:
            # To prevent dependency on langchain_text_splitters unless needed.
            from langchain_text_splitters import MarkdownHeaderTextSplitter
        except ImportError as e:
            logger.error(f'Error importing langchain_text_splitters to insert web page into knowledge base: {e}')
            raise e

        if not default_headers:
            default_headers = _DEFAULT_MARKDOWN_HEADERS_TO_SPLIT_ON

        websites_df = get_all_websites(urls, limit=_DEFAULT_WEB_CRAWL_LIMIT, crawl_depth=crawl_depth, filters=filters)
        markdown_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=default_headers)

        # Collect all documents for batch preprocessing
        raw_documents = []

        def process_row(row):
            docs = markdown_splitter.split_text(row['text_content'])
            for doc in docs:
                raw_documents.append({
                    'content': doc.page_content,
                    'metadata': {'url': row['url']}
                })

        websites_df.apply(process_row, axis=1)

        # Preprocess and insert
        if raw_documents:
            processed_chunks = self._preprocess_documents(raw_documents)
            df = self._chunks_to_dataframe(processed_chunks)
            self.table.insert(df)

    def process_and_insert_rows(self, rows: List[Dict]):
        """Process and insert raw data rows with preprocessing"""
        if not rows:
            return

        # Convert rows to standard document format while preserving IDs
        raw_documents = []
        for row in rows:
            doc = {
                'content': row.get('content', ''),
                'id': row.get('id'),  # Preserve original ID
                'metadata': {k: v for k, v in row.items() if k not in ['content', 'id']}
            }
            raw_documents.append(doc)

        processed_chunks = self._preprocess_documents(raw_documents)

        # Ensure IDs are preserved after preprocessing
        for i, chunk in enumerate(processed_chunks):
            if chunk.id is None and i < len(raw_documents):
                chunk.id = raw_documents[i]['id']

        df = self._chunks_to_dataframe(processed_chunks)
        self.table.insert(df)


def update_knowledge_base_with_preprocessing(
        table: KnowledgeBaseTable,
        files: List[str] = None,
        urls: List[str] = None,
        rows: List[Dict] = None,
        query: str = None,
        project_name: str = None,
        crawl_depth: int = 1,
        filters: List[str] = None,
        preprocessing_config: Optional[dict] = None
):
    """Main function to update knowledge base with preprocessing support"""

    # Need to configure preprocessing on the table first if config exists
    if preprocessing_config:
        table.configure_preprocessing(preprocessing_config)

    # Pass preprocessing_config, not undefined 'config'
    kb_processor = PreprocessingKnowledgeBase(table, preprocessing_config)

    # Process files
    if files:
        kb_processor.process_and_insert_files(files)

    # Process web pages
    if urls:
        kb_processor.process_and_insert_web_pages(urls, crawl_depth, filters)

    # Process raw data rows
    if rows:
        kb_processor.process_and_insert_rows(rows)

    # Process query results
    if query and project_name:
        kb_processor.process_and_insert_query_result(query, project_name)
