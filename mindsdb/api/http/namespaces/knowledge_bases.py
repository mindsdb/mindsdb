from http import HTTPStatus
from typing import List, Dict

from flask import request
from flask_restx import Resource

import pandas as pd

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.http.utils import http_error
from mindsdb.interfaces.knowledge_base.data_source_config import S3Config
from mindsdb.integrations.utilities.rag.s3_knowledge_base import get_filtered_files, save_s3_file_to_tempfile
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.integrations.handlers.web_handler.urlcrawl_helpers import get_all_websites
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.utilities.rag.loaders.file_loader import FileLoader
from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseTable
from mindsdb.utilities import log

logger = log.getLogger(__name__)

_DEFAULT_MARKDOWN_HEADERS_TO_SPLIT_ON = [
    ("#", "Header 1"),
    ("##", "Header 2"),
    ("###", "Header 3"),
]

_DEFAULT_WEB_CRAWL_LIMIT = 100


def _insert_file_into_knowledge_base(table: KnowledgeBaseTable, file_name: str):
    file_controller = FileController()
    splitter = FileSplitter(FileSplitterConfig())
    file_path = file_controller.get_file_path(file_name)
    loader = FileLoader(file_path)
    split_docs = []
    for doc in loader.lazy_load():
        split_docs += splitter.split_documents([doc])
    doc_objs = []
    for split_doc in split_docs:
        doc_objs.append({
            'content': split_doc.page_content,
        })
    docs_df = pd.DataFrame.from_records(doc_objs)
    # Insert documents into KB
    table.insert(docs_df)


def _insert_web_pages_into_knowledge_base(table: KnowledgeBaseTable, urls: List[str], crawl_depth: int = 1, filters: List[str] = None):
    try:
        # To prevent dependency on langchain_text_splitters unless needed.
        from langchain_text_splitters import MarkdownHeaderTextSplitter
    except ImportError as e:
        logger.error(f'Error importing langchain_text_splitters to insert web page into knowledge base: {e}')
        raise e

    websites_df = get_all_websites(urls, limit=_DEFAULT_WEB_CRAWL_LIMIT, crawl_depth=crawl_depth, filters=filters)
    # Text content is treated as markdown.
    markdown_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=_DEFAULT_MARKDOWN_HEADERS_TO_SPLIT_ON)

    def append_row_documents(row, all_docs):
        docs = markdown_splitter.split_text(row['text_content'])
        for doc in docs:
            # Link the URL to each web page chunk.
            doc.metadata['url'] = row['url']
        all_docs += docs

    all_docs = []
    websites_df.apply(lambda row: append_row_documents(row, all_docs), axis=1)
    # Convert back to a DF.
    doc_objs = []
    for doc in all_docs:
        doc_objs.append({
            'content': doc.page_content,
            'url': doc.metadata['url']
        })
    docs_df = pd.DataFrame.from_records(doc_objs)
    # Insert documents into KB.
    table.insert(docs_df)


def _insert_from_s3_config_into_knowledge_base(table: KnowledgeBaseTable, s3_config: S3Config):
    files: List[Dict] = get_filtered_files(s3_config=s3_config)  # dict key format: bucket, key, last_modified, size
    for file_config in files:
        bucket = file_config['bucket']
        key = file_config['key']

        # Save the S3 file to a temporary file
        temp_file = save_s3_file_to_tempfile(s3_config=s3_config, bucket_name=bucket, file_key=key)

        if temp_file:
            # Use FileLoader to load and split the documents
            loader = FileLoader(temp_file.name)
            splitter = FileSplitter(FileSplitterConfig())
            split_docs = []

            # Lazily load the documents from the temporary file
            for doc in loader.lazy_load():
                split_docs += [{'page_number': i, 'document' : x} for i, x in enumerate(splitter.split_documents([doc]))]

            # Prepare documents for insertion
            doc_objs = []
            for split_doc in split_docs:
                content = split_doc['document'].page_content
                page_number = split_doc['page_number']
                doc_objs.append({'content':content, 'id':f'{bucket},{page_number},{key}'})
            docs_df = pd.DataFrame.from_records(doc_objs)

            # Insert documents into the knowledge base
            table.insert(docs_df)
            # clean up the temporary file
            temp_file.close()

        else:
            logger.error(f"Failed to save file {key} from bucket {bucket} to temporary storage.")


@ns_conf.route('/<project_name>/knowledge_bases/<knowledge_base_name>')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('knowledge_base_name', 'Name of the knowledge_base')
class KnowledgeBaseResource(Resource):
    @ns_conf.doc('update_knowledge_base')
    @api_endpoint_metrics('PUT', '/knowledge_bases/knowledge_base')
    def put(self, project_name: str, knowledge_base_name: str):
        '''Updates a knowledge base.'''
        # Check for required parameters.
        if 'knowledge_base' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "knowledge_base" parameter in PUT body'
            )
        session = SessionController()
        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except ValueError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )
        try:
            table = session.kb_controller.get_table(knowledge_base_name, project.id)
        except ValueError:
            # Knowledge Base must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Knowledge Base not found',
                f'Knowledge Base with name {knowledge_base_name} does not exist'
            )

        kb = request.json['knowledge_base']
        if 's3' in kb and kb.get('s3'):
            s3_config: S3Config
            for config in kb['s3']:
                s3_config = S3Config(**config)
                _insert_from_s3_config_into_knowledge_base(table=table,s3_config=s3_config)

        files = kb.get('files', [])
        urls = kb.get('urls', [])
        # Optional params for web pages.
        crawl_depth = kb.get('crawl_depth', 1)
        filters = kb.get('filters', [])

        # Load, split, & embed files into Knowledge Base.
        for file_name in files:
            _insert_file_into_knowledge_base(table, file_name)
        # Crawl, split, & embed web pages into Knowledge Base.
        _insert_web_pages_into_knowledge_base(table, urls, crawl_depth=crawl_depth, filters=filters)
        return '', HTTPStatus.OK
