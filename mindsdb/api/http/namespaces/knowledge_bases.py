from http import HTTPStatus

from flask import request
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.exceptions import ExecutorException
from mindsdb.api.http.utils import http_error
from mindsdb.interfaces.knowledge_base.preprocess.preprocess import (update_knowledge_base_with_preprocessing,
                                                                     _insert_select_query_result_into_knowledge_base)
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities import log
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_MODEL, DEFAULT_RAG_PROMPT_TEMPLATE


from mindsdb_sql.parser.ast import Identifier

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
        except EntityNotExistsError:
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

        # Extract parameters
        files = kb.get('files', [])
        urls = kb.get('urls', [])
        query = kb.get('query')
        rows = kb.get('rows', [])

        # Optional params for web pages.
        crawl_depth = kb.get('crawl_depth', 1)
        filters = kb.get('filters', [])

        # get chunk preprocessing config
        preprocessing_config = kb.get('preprocessing', None)

        # Handle SQL query if present
        if query:
            try:
                _insert_select_query_result_into_knowledge_base(query, table, project_name)
            except ExecutorException as e:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Invalid SELECT query',
                    f'Executing "query" failed. Needs to be a valid SELECT statement that returns data: {str(e)}'
                )

        # Process all other inputs with preprocessing support
        try:
            update_knowledge_base_with_preprocessing(
                table=table,
                files=files,
                urls=urls,
                rows=rows,
                crawl_depth=crawl_depth,
                filters=filters,
                preprocessing_config=preprocessing_config
            )
        except Exception as e:
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


@ns_conf.route('/<project_name>/knowledge_bases/<knowledge_base_name>/completions')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('knowledge_base_name', 'Name of the knowledge_base')
class KnowledgeBaseCompletions(Resource):
    @ns_conf.doc('knowledge_base_completions')
    @api_endpoint_metrics('POST', '/knowledge_bases/knowledge_base/completions')
    def post(self, project_name, knowledge_base_name):
        """
        Add support for LLM generation on the response from knowledge base
        """
        # Check for required parameters.
        if 'knowledge_base' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "knowledge_base" parameter in POST body'
            )

        # Check for required parameters
        query = request.json.get('query')
        if query is None:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "query" parameter in POST body'
            )

            logger.error('Missing parameter "query" in POST body')

        llm_model = request.json.get('llm_model')
        if llm_model is None:
            logger.warn(f'Missing parameter "llm_model" in POST body, using default llm_model {DEFAULT_LLM_MODEL}')

        prompt_template = request.json.get('prompt_template')
        if prompt_template is None:
            logger.warn(f'Missing parameter "prompt_template" in POST body, using default prompt template {DEFAULT_RAG_PROMPT_TEMPLATE}')

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
            logger.error("Project not found, please check the project name exists")

        # Check if knowledge base exists
        table = session.kb_controller.get_table(knowledge_base_name, project.id)
        if table is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Knowledge Base not found',
                f'Knowledge Base with name {knowledge_base_name} does not exist'
            )

            logger.error("Knowledge Base not found, please check the knowledge base name exists")

        # Get retrieval config, if set
        retrieval_config = request.json.get('retrieval_config', {})
        if not retrieval_config:
            logger.warn('No retrieval config provided, using default retrieval config')

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
        rag_pipeline = table.build_rag_pipeline(retrieval_config)

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
