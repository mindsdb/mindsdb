from http import HTTPStatus

from flask import request
from flask_restx import Resource
from langchain_text_splitters import MarkdownHeaderTextSplitter

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.exceptions import ExecutorException
from mindsdb.api.http.utils import http_error

from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.interfaces.knowledge_base.preprocessing.constants import (
    DEFAULT_CONTEXT_DOCUMENT_LIMIT,
    DEFAULT_CRAWL_DEPTH, DEFAULT_WEB_FILTERS,
    DEFAULT_MARKDOWN_HEADERS, DEFAULT_WEB_CRAWL_LIMIT
)
from mindsdb.interfaces.knowledge_base.preprocessing.document_loader import DocumentLoader
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseTable
from mindsdb.utilities import log
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_MODEL, DEFAULT_RAG_PROMPT_TEMPLATE


from mindsdb_sql_parser.ast import Identifier

logger = log.getLogger(__name__)


@ns_conf.route('/<project_name>/knowledge_bases')
class KnowledgeBasesResource(Resource):
    @ns_conf.doc('list_knowledge_bases')
    @api_endpoint_metrics('GET', '/knowledge_bases')
    def get(self, project_name):
        '''List all knowledge bases'''
        session = SessionController()
        project_controller = ProjectController()
        try:
            _ = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        # KB Controller already returns dict.
        return session.kb_controller.list(project_name)

    @ns_conf.doc('create_knowledge_base')
    @api_endpoint_metrics('POST', '/knowledge_bases')
    def post(self, project_name):
        '''Create a knowledge base'''

        # Check for required parameters.
        if 'knowledge_base' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "knowledge_base" parameter in POST body'
            )

        knowledge_base = request.json['knowledge_base']
        # Explicitly require embedding model & vector database.
        required_fields = ['name', 'model']
        for field in required_fields:
            if field not in knowledge_base:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Missing parameter',
                    f'Must provide "{field}" field in "knowledge_base"'
                )
        if 'storage' in knowledge_base:
            if 'database' not in knowledge_base['storage'] or 'table' not in knowledge_base['storage']:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Missing parameter',
                    'Must provide "database" and "table" field in "storage" param'
                )

        session = SessionController()
        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        kb_name = knowledge_base.get('name')
        existing_kb = session.kb_controller.get(kb_name, project.id)
        if existing_kb is not None:
            # Knowledge Base must not exist.
            return http_error(
                HTTPStatus.CONFLICT,
                'Knowledge Base already exists',
                f'Knowledge Base with name {kb_name} already exists'
            )

        embedding_model_identifier = None
        if knowledge_base.get('model'):
            embedding_model_identifier = Identifier(parts=[knowledge_base['model']])

        storage = knowledge_base.get('storage')
        embedding_table_identifier = None
        if storage is not None:
            embedding_table_identifier = Identifier(parts=[storage['database'], storage['table']])

        try:
            new_kb = session.kb_controller.add(
                kb_name,
                project.name,
                embedding_model_identifier,
                embedding_table_identifier,
                params=knowledge_base.get('params', {}),
                preprocessing_config=knowledge_base.get('preprocessing')
            )
        except ValueError as e:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Invalid preprocessing configuration',
                str(e)
            )

        return new_kb.as_dict(), HTTPStatus.CREATED


@ns_conf.route('/<project_name>/knowledge_bases/<knowledge_base_name>')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('knowledge_base_name', 'Name of the knowledge_base')
class KnowledgeBaseResource(Resource):
    @ns_conf.doc('get_knowledge_base')
    @api_endpoint_metrics('GET', '/knowledge_bases/knowledge_base')
    def get(self, project_name, knowledge_base_name):
        '''Gets a knowledge base by name'''
        session = SessionController()
        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        existing_kb = session.kb_controller.get(knowledge_base_name, project.id)
        if existing_kb is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Knowledge Base not found',
                f'Knowledge Base with name {knowledge_base_name} does not exist'
            )
        return existing_kb.as_dict()

    @ns_conf.doc('update_knowledge_base')
    @api_endpoint_metrics('PUT', '/knowledge_bases/knowledge_base')
    def put(self, project_name: str, knowledge_base_name: str):
        '''Updates a knowledge base with optional preprocessing.'''

        # Check for required parameters
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
        except EntityNotExistsError:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        try:
            kb_data = request.json['knowledge_base']

            # Retrieve the knowledge base table for updates
            table = session.kb_controller.get_table(knowledge_base_name, project.id, params=kb_data.get('params'))
            if table is None:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Knowledge Base not found',
                    f'Knowledge Base with name {knowledge_base_name} does not exist'
                )

            # Set up dependencies for DocumentLoader
            file_controller = FileController()
            file_splitter_config = FileSplitterConfig()
            file_splitter = FileSplitter(file_splitter_config)
            markdown_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=DEFAULT_MARKDOWN_HEADERS)
            mysql_proxy = FakeMysqlProxy()

            # Initialize DocumentLoader with required components
            document_loader = DocumentLoader(
                file_controller=file_controller,
                file_splitter=file_splitter,
                markdown_splitter=markdown_splitter,
                mysql_proxy=mysql_proxy
            )

            # Configure table with dependencies
            table.document_loader = document_loader

            # Update preprocessing configuration if provided
            if 'preprocessing' in kb_data:
                table.configure_preprocessing(kb_data['preprocessing'])

            # Process raw data rows if provided
            if kb_data.get('rows'):
                table.insert_rows(kb_data['rows'])

            # Process files if specified
            if kb_data.get('files'):
                table.insert_files(kb_data['files'])

            # Process web pages if URLs provided
            if kb_data.get('urls'):
                table.insert_web_pages(
                    urls=kb_data['urls'],
                    limit=kb_data.get('limit') or DEFAULT_WEB_CRAWL_LIMIT,
                    crawl_depth=kb_data.get('crawl_depth', DEFAULT_CRAWL_DEPTH),
                    filters=kb_data.get('filters', DEFAULT_WEB_FILTERS)
                )

            # Process query if provided
            if kb_data.get('query'):
                table.insert_query_result(kb_data['query'], project_name)

        except ExecutorException as e:
            logger.error(f'Error during preprocessing and insertion: {str(e)}')
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Invalid SELECT query',
                f'Executing "query" failed. Needs to be a valid SELECT statement that returns data: {str(e)}'
            )

        except Exception as e:
            logger.error(f'Error during preprocessing and insertion: {str(e)}')
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Preprocessing Error',
                f'Error during preprocessing and insertion: {str(e)}'
            )

        return '', HTTPStatus.OK

    @ns_conf.doc('delete_knowledge_base')
    @api_endpoint_metrics('DELETE', '/knowledge_bases/knowledge_base')
    def delete(self, project_name: str, knowledge_base_name: str):
        '''Deletes a knowledge base.'''
        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        session_controller = SessionController()
        existing_kb = session_controller.kb_controller.get(knowledge_base_name, project.id)
        if existing_kb is None:
            # Knowledge Base must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Knowledge Base not found',
                f'Knowledge Base with name {knowledge_base_name} does not exist'
            )

        session_controller.kb_controller.delete(knowledge_base_name, project_name)
        return '', HTTPStatus.NO_CONTENT


def _handle_chat_completion(knowledge_base_table: KnowledgeBaseTable, request):
    # Check for required parameters
    query = request.json.get('query')

    llm_model = request.json.get('llm_model')
    if llm_model is None:
        logger.warning(f'Missing parameter "llm_model" in POST body, using default llm_model {DEFAULT_LLM_MODEL}')

    prompt_template = request.json.get('prompt_template')
    if prompt_template is None:
        logger.warning(f'Missing parameter "prompt_template" in POST body, using default prompt template {DEFAULT_RAG_PROMPT_TEMPLATE}')

    # Get retrieval config, if set
    retrieval_config = request.json.get('retrieval_config', {})
    if not retrieval_config:
        logger.warning('No retrieval config provided, using default retrieval config')

    # add llm model to retrieval config
    if llm_model is not None:
        retrieval_config['llm_model_name'] = llm_model

    # add prompt template to retrieval config
    if prompt_template is not None:
        retrieval_config['rag_prompt_template'] = prompt_template

    # add llm provider to retrieval config if set
    llm_provider = request.json.get('model_provider')
    if llm_provider is not None:
        retrieval_config['llm_provider'] = llm_provider

    # build rag pipeline
    rag_pipeline = knowledge_base_table.build_rag_pipeline(retrieval_config)

    # get response from rag pipeline
    rag_response = rag_pipeline(query)
    response = {
        'message': {
            'content': rag_response.get('answer'),
            'context': rag_response.get('context'),
            'role': 'assistant'
        }
    }

    return response


def _handle_context_completion(knowledge_base_table: KnowledgeBaseTable, request):
    # Used for semantic search.
    query = request.json.get('query')
    # Keyword search.
    keywords = request.json.get('keywords')
    # Metadata search.
    metadata = request.json.get('metadata')
    # Maximum amount of documents to return as context.
    limit = request.json.get('limit', DEFAULT_CONTEXT_DOCUMENT_LIMIT)

    # Use default distance function & column names for ID, content, & metadata, to keep things simple.
    hybrid_search_df = knowledge_base_table.hybrid_search(
        query,
        keywords=keywords,
        metadata=metadata
    )

    num_documents = len(hybrid_search_df.index)
    context_documents = []
    for i in range(limit):
        if i >= num_documents:
            break
        row = hybrid_search_df.iloc[i]
        context_documents.append({
            'id': row['id'],
            'content': row['content'],
            'rank': row['rank']
        })

    return {
        'documents': context_documents
    }


@ns_conf.route('/<project_name>/knowledge_bases/<knowledge_base_name>/completions')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('knowledge_base_name', 'Name of the knowledge_base')
class KnowledgeBaseCompletions(Resource):
    @ns_conf.doc('knowledge_base_completions')
    @api_endpoint_metrics('POST', '/knowledge_bases/knowledge_base/completions')
    def post(self, project_name, knowledge_base_name):
        """
        Add support for LLM generation on the response from knowledge base. Default completion type is 'chat' unless specified.
        """
        if request.json.get('query') is None:
            # "query" is used for semantic search for both completion types.
            logger.error('Missing parameter "query" in POST body')
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "query" parameter in POST body'
            )

        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            logger.error("Project not found, please check the project name exists")
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        session = SessionController()
        # Check if knowledge base exists
        table = session.kb_controller.get_table(knowledge_base_name, project.id)
        if table is None:
            logger.error("Knowledge Base not found, please check the knowledge base name exists")
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Knowledge Base not found',
                f'Knowledge Base with name {knowledge_base_name} does not exist'
            )

        completion_type = request.json.get('type', 'chat')
        if completion_type == 'context':
            return _handle_context_completion(table, request)
        if completion_type == 'chat':
            return _handle_chat_completion(table, request)
        return http_error(
            HTTPStatus.BAD_REQUEST,
            'Invalid parameter',
            f'Completion type must be one of: "context", "chat". Received {completion_type}'
        )


@ns_conf.route('/<project_name>/knowledge_bases/<knowledge_base_name>/evaluate')
class KnowledgeBaseEvaluate(Resource):
    @ns_conf.doc('evaluate_knowledge_base')
    @api_endpoint_metrics('POST', '/knowledge_bases/evaluate')
    def post(self, project_name, knowledge_base_name):
        """
        Evaluate a knowledge base against a set of test queries

        Expected JSON body:
        {
            "test_queries": [
                {"query": "query text", "expected_ids": ["id1", "id2"]},
                ...
            ],
            "metrics": ["precision", "recall", "mrr"],  # Optional
            "top_k": 5,  # Optional
            "test_set_name": "test_set_name",  # Optional
            "user_notes": "notes about the evaluation"  # Optional
        }

        Or with a reference to a test data table:
        {
            "test_data": "project_name.table_name",
            "query_column": "question",
            "expected_column": "relevant_doc_ids",
            "metrics": ["precision", "recall", "mrr"],  # Optional
            "top_k": 5,  # Optional
            "test_set_name": "test_set_name",  # Optional
            "user_notes": "notes about the evaluation"  # Optional
        }
        """
        session = SessionController()
        project_controller = ProjectController()

        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        try:
            kb_controller = session.knowledge_base_controller
            # Check if knowledge base exists
            kb_controller.get(knowledge_base_name, project.id)
        except EntityNotExistsError:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Knowledge base not found',
                f'Knowledge base with name {knowledge_base_name} does not exist'
            )

        data = request.json
        if not data:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing request body',
                'Request body must contain test queries or test data reference'
            )

        # Process test queries from request or from a reference table
        test_queries = data.get('test_queries')
        test_data_ref = data.get('test_data')

        if test_queries is None and test_data_ref is None:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing test data',
                'Request must contain either "test_queries" or "test_data" field'
            )

        # If test_data is provided, load test queries from the referenced table
        if test_data_ref:
            query_column = data.get('query_column')
            expected_column = data.get('expected_column')

            if not query_column or not expected_column:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Missing column specifications',
                    'When using test_data, both query_column and expected_column must be specified'
                )

            # Parse test_data_ref (format: "project_name.table_name")
            parts = test_data_ref.split('.')
            if len(parts) != 2:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Invalid test_data format',
                    'test_data must be in format "project_name.table_name"'
                )

            test_project_name, test_table_name = parts

            try:
                # Create SQL query to fetch test data
                sql = f"SELECT {query_column}, {expected_column} FROM {test_project_name}.{test_table_name}"

                # Execute query using FakeMysqlProxy to get the data
                mysql_proxy = FakeMysqlProxy(session)
                result = mysql_proxy.process_query(sql)

                if not result or not result.data:
                    return http_error(
                        HTTPStatus.BAD_REQUEST,
                        'Empty test data',
                        f'No data found in {test_data_ref}'
                    )

                # Convert result to test_queries format
                test_queries = []
                for row in result.data:
                    # Parse expected_ids from string if needed
                    expected_ids = row[expected_column]
                    if isinstance(expected_ids, str):
                        try:
                            # Try to parse as JSON
                            import json
                            expected_ids = json.loads(expected_ids)
                        except json.JSONDecodeError:
                            # If not JSON, split by comma
                            expected_ids = [id.strip() for id in expected_ids.split(',')]

                    test_queries.append({
                        "query": row[query_column],
                        "expected_ids": expected_ids
                    })
            except Exception as e:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Error loading test data',
                    str(e)
                )

        # Extract optional parameters
        metrics = data.get('metrics', ["precision", "recall", "mrr"])
        top_k = data.get('top_k', 5)
        test_set_name = data.get('test_set_name')
        user_notes = data.get('user_notes')

        try:
            # Run evaluation
            result = kb_controller.evaluate(
                name=knowledge_base_name,
                project_id=project.id,
                test_queries=test_queries,
                metrics=metrics,
                top_k=top_k,
                test_set_name=test_set_name,
                user_notes=user_notes
            )
            return result, HTTPStatus.OK
        except Exception as e:
            logger.error(f"Error during knowledge base evaluation: {e}", exc_info=True)
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Evaluation error',
                str(e)
            )


@ns_conf.route('/<project_name>/knowledge_bases/<knowledge_base_name>/evaluations')
class KnowledgeBaseEvaluations(Resource):
    @ns_conf.doc('get_knowledge_base_evaluations')
    @api_endpoint_metrics('GET', '/knowledge_bases/evaluations')
    def get(self, project_name, knowledge_base_name):
        """List evaluation history for a knowledge base"""
        session = SessionController()
        project_controller = ProjectController()

        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        try:
            kb_controller = session.knowledge_base_controller
            # Get evaluation history
            limit = request.args.get('limit', type=int)
            evaluations = kb_controller.get_evaluations(
                name=knowledge_base_name,
                project_id=project.id,
                limit=limit
            )
            return evaluations, HTTPStatus.OK
        except EntityNotExistsError:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Knowledge base not found',
                f'Knowledge base with name {knowledge_base_name} does not exist'
            )
        except Exception as e:
            logger.error(f"Error retrieving knowledge base evaluations: {e}", exc_info=True)
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Error retrieving evaluations',
                str(e)
            )


@ns_conf.route('/<project_name>/knowledge_bases/evaluations/<int:evaluation_id>')
class KnowledgeBaseEvaluationResource(Resource):
    @ns_conf.doc('get_knowledge_base_evaluation')
    @api_endpoint_metrics('GET', '/knowledge_bases/evaluations/id')
    def get(self, project_name, evaluation_id):
        """Get a specific evaluation by ID"""
        session = SessionController()

        try:
            kb_controller = session.knowledge_base_controller
            evaluation = kb_controller.get_evaluation(evaluation_id)

            # Check if evaluation belongs to the specified project
            project_controller = ProjectController()
            project = project_controller.get(name=project_name)

            if evaluation.get('project_id') != project.id:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Evaluation not found',
                    f'Evaluation with ID {evaluation_id} does not exist in project {project_name}'
                )

            return evaluation, HTTPStatus.OK
        except EntityNotExistsError as e:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Evaluation not found',
                str(e)
            )
        except Exception as e:
            logger.error(f"Error retrieving knowledge base evaluation: {e}", exc_info=True)
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Error retrieving evaluation',
                str(e)
            )
