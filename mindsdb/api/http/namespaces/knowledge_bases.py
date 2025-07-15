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
    DEFAULT_CRAWL_DEPTH,
    DEFAULT_WEB_FILTERS,
    DEFAULT_MARKDOWN_HEADERS,
    DEFAULT_WEB_CRAWL_LIMIT,
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


@ns_conf.route("/<project_name>/knowledge_bases")
class KnowledgeBasesResource(Resource):
    @ns_conf.doc("list_knowledge_bases")
    @api_endpoint_metrics("GET", "/knowledge_bases")
    def get(self, project_name):
        """List all knowledge bases"""
        session = SessionController()
        project_controller = ProjectController()
        try:
            _ = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        # KB Controller already returns dict.
        return session.kb_controller.list(project_name)

    @ns_conf.doc("create_knowledge_base")
    @api_endpoint_metrics("POST", "/knowledge_bases")
    def post(self, project_name):
        """Create a knowledge base"""

        # Check for required parameters.
        if "knowledge_base" not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST, "Missing parameter", 'Must provide "knowledge_base" parameter in POST body'
            )

        knowledge_base = request.json["knowledge_base"]
        # Explicitly require embedding model & vector database.
        required_fields = ["name"]
        for field in required_fields:
            if field not in knowledge_base:
                return http_error(
                    HTTPStatus.BAD_REQUEST, "Missing parameter", f'Must provide "{field}" field in "knowledge_base"'
                )
        if "storage" in knowledge_base:
            if "database" not in knowledge_base["storage"] or "table" not in knowledge_base["storage"]:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    "Missing parameter",
                    'Must provide "database" and "table" field in "storage" param',
                )

        session = SessionController()
        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        kb_name = knowledge_base.get("name")
        existing_kb = session.kb_controller.get(kb_name, project.id)
        if existing_kb is not None:
            # Knowledge Base must not exist.
            return http_error(
                HTTPStatus.CONFLICT,
                "Knowledge Base already exists",
                f"Knowledge Base with name {kb_name} already exists",
            )

        # Legacy: Support for embedding model identifier.
        # embedding_model_identifier = None
        # if knowledge_base.get('model'):
        #     embedding_model_identifier = Identifier(parts=[knowledge_base['model']])

        storage = knowledge_base.get("storage")
        embedding_table_identifier = None
        if storage is not None:
            embedding_table_identifier = Identifier(parts=[storage["database"], storage["table"]])

        params = knowledge_base.get("params", {})

        optional_parameter_fields = [
            "embedding_model",
            "reranking_model",
            "content_columns",
            "metadata_columns",
            "id_column",
        ]

        for field in optional_parameter_fields:
            if field in knowledge_base:
                params[field] = knowledge_base[field]

        try:
            new_kb = session.kb_controller.add(
                kb_name,
                project.name,
                embedding_table_identifier,
                params=params,
                preprocessing_config=knowledge_base.get("preprocessing"),
            )
        except ValueError as e:
            return http_error(HTTPStatus.BAD_REQUEST, "Invalid preprocessing configuration", str(e))

        return new_kb.as_dict(session.show_secrets), HTTPStatus.CREATED


@ns_conf.route("/<project_name>/knowledge_bases/<knowledge_base_name>")
@ns_conf.param("project_name", "Name of the project")
@ns_conf.param("knowledge_base_name", "Name of the knowledge_base")
class KnowledgeBaseResource(Resource):
    @ns_conf.doc("get_knowledge_base")
    @api_endpoint_metrics("GET", "/knowledge_bases/knowledge_base")
    def get(self, project_name, knowledge_base_name):
        """Gets a knowledge base by name"""
        session = SessionController()
        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        existing_kb = session.kb_controller.get(knowledge_base_name, project.id)
        if existing_kb is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                "Knowledge Base not found",
                f"Knowledge Base with name {knowledge_base_name} does not exist",
            )
        return existing_kb.as_dict(session.show_secrets), HTTPStatus.OK

    @ns_conf.doc("update_knowledge_base")
    @api_endpoint_metrics("PUT", "/knowledge_bases/knowledge_base")
    def put(self, project_name: str, knowledge_base_name: str):
        """Updates a knowledge base with optional preprocessing."""

        # Check for required parameters
        if "knowledge_base" not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST, "Missing parameter", 'Must provide "knowledge_base" parameter in PUT body'
            )

        session = SessionController()
        project_controller = ProjectController()

        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        try:
            kb_data = request.json["knowledge_base"]

            # Retrieve the knowledge base table for updates
            table = session.kb_controller.get_table(knowledge_base_name, project.id, params=kb_data.get("params"))
            if table is None:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    "Knowledge Base not found",
                    f"Knowledge Base with name {knowledge_base_name} does not exist",
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
                mysql_proxy=mysql_proxy,
            )

            # Configure table with dependencies
            table.document_loader = document_loader

            # Update preprocessing configuration if provided
            if "preprocessing" in kb_data:
                table.configure_preprocessing(kb_data["preprocessing"])

            # Process raw data rows if provided
            if kb_data.get("rows"):
                table.insert_rows(kb_data["rows"])

            # Process files if specified
            if kb_data.get("files"):
                table.insert_files(kb_data["files"])

            # Process web pages if URLs provided
            if kb_data.get("urls"):
                table.insert_web_pages(
                    urls=kb_data["urls"],
                    limit=kb_data.get("limit") or DEFAULT_WEB_CRAWL_LIMIT,
                    crawl_depth=kb_data.get("crawl_depth", DEFAULT_CRAWL_DEPTH),
                    filters=kb_data.get("filters", DEFAULT_WEB_FILTERS),
                )

            # Process query if provided
            if kb_data.get("query"):
                table.insert_query_result(kb_data["query"], project_name)

        except ExecutorException as e:
            logger.error(f"Error during preprocessing and insertion: {str(e)}")
            return http_error(
                HTTPStatus.BAD_REQUEST,
                "Invalid SELECT query",
                f'Executing "query" failed. Needs to be a valid SELECT statement that returns data: {str(e)}',
            )

        except Exception as e:
            logger.error(f"Error during preprocessing and insertion: {str(e)}")
            return http_error(
                HTTPStatus.BAD_REQUEST, "Preprocessing Error", f"Error during preprocessing and insertion: {str(e)}"
            )

        return "", HTTPStatus.OK

    @ns_conf.doc("delete_knowledge_base")
    @api_endpoint_metrics("DELETE", "/knowledge_bases/knowledge_base")
    def delete(self, project_name: str, knowledge_base_name: str):
        """Deletes a knowledge base."""
        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        session_controller = SessionController()
        existing_kb = session_controller.kb_controller.get(knowledge_base_name, project.id)
        if existing_kb is None:
            # Knowledge Base must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                "Knowledge Base not found",
                f"Knowledge Base with name {knowledge_base_name} does not exist",
            )

        session_controller.kb_controller.delete(knowledge_base_name, project_name)
        return "", HTTPStatus.NO_CONTENT


def _handle_chat_completion(knowledge_base_table: KnowledgeBaseTable, request):
    # Check for required parameters
    query = request.json.get("query")

    llm_model = request.json.get("llm_model")
    if llm_model is None:
        logger.warning(f'Missing parameter "llm_model" in POST body, using default llm_model {DEFAULT_LLM_MODEL}')

    prompt_template = request.json.get("prompt_template")
    if prompt_template is None:
        logger.warning(
            f'Missing parameter "prompt_template" in POST body, using default prompt template {DEFAULT_RAG_PROMPT_TEMPLATE}'
        )

    # Get retrieval config, if set
    retrieval_config = request.json.get("retrieval_config", {})
    if not retrieval_config:
        logger.warning("No retrieval config provided, using default retrieval config")

    # add llm model to retrieval config
    if llm_model is not None:
        retrieval_config["llm_model_name"] = llm_model

    # add prompt template to retrieval config
    if prompt_template is not None:
        retrieval_config["rag_prompt_template"] = prompt_template

    # add llm provider to retrieval config if set
    llm_provider = request.json.get("model_provider")
    if llm_provider is not None:
        retrieval_config["llm_provider"] = llm_provider

    # build rag pipeline
    rag_pipeline = knowledge_base_table.build_rag_pipeline(retrieval_config)

    # get response from rag pipeline
    rag_response = rag_pipeline(query)
    response = {
        "message": {"content": rag_response.get("answer"), "context": rag_response.get("context"), "role": "assistant"}
    }

    return response


def _handle_context_completion(knowledge_base_table: KnowledgeBaseTable, request):
    # Used for semantic search.
    query = request.json.get("query")
    # Keyword search.
    keywords = request.json.get("keywords")
    # Metadata search.
    metadata = request.json.get("metadata")
    # Maximum amount of documents to return as context.
    limit = request.json.get("limit", DEFAULT_CONTEXT_DOCUMENT_LIMIT)

    # Use default distance function & column names for ID, content, & metadata, to keep things simple.
    hybrid_search_df = knowledge_base_table.hybrid_search(query, keywords=keywords, metadata=metadata)

    num_documents = len(hybrid_search_df.index)
    context_documents = []
    for i in range(limit):
        if i >= num_documents:
            break
        row = hybrid_search_df.iloc[i]
        context_documents.append({"id": row["id"], "content": row["content"], "rank": row["rank"]})

    return {"documents": context_documents}


@ns_conf.route("/<project_name>/knowledge_bases/<knowledge_base_name>/completions")
@ns_conf.param("project_name", "Name of the project")
@ns_conf.param("knowledge_base_name", "Name of the knowledge_base")
class KnowledgeBaseCompletions(Resource):
    @ns_conf.doc("knowledge_base_completions")
    @api_endpoint_metrics("POST", "/knowledge_bases/knowledge_base/completions")
    def post(self, project_name, knowledge_base_name):
        """
        Add support for LLM generation on the response from knowledge base. Default completion type is 'chat' unless specified.
        """
        if request.json.get("query") is None:
            # "query" is used for semantic search for both completion types.
            logger.error('Missing parameter "query" in POST body')
            return http_error(
                HTTPStatus.BAD_REQUEST, "Missing parameter", 'Must provide "query" parameter in POST body'
            )

        project_controller = ProjectController()
        try:
            project = project_controller.get(name=project_name)
        except EntityNotExistsError:
            # Project must exist.
            logger.error("Project not found, please check the project name exists")
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        session = SessionController()
        # Check if knowledge base exists
        table = session.kb_controller.get_table(knowledge_base_name, project.id)
        if table is None:
            logger.error("Knowledge Base not found, please check the knowledge base name exists")
            return http_error(
                HTTPStatus.NOT_FOUND,
                "Knowledge Base not found",
                f"Knowledge Base with name {knowledge_base_name} does not exist",
            )

        completion_type = request.json.get("type", "chat")
        if completion_type == "context":
            return _handle_context_completion(table, request)
        if completion_type == "chat":
            return _handle_chat_completion(table, request)
        return http_error(
            HTTPStatus.BAD_REQUEST,
            "Invalid parameter",
            f'Completion type must be one of: "context", "chat". Received {completion_type}',
        )
