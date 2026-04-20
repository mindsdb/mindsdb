from http import HTTPStatus

from flask import request
from flask_restx import Resource
from mindsdb_sql_parser.ast import Identifier

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.exceptions import ExecutorException
from mindsdb.api.http.utils import http_error

from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.interfaces.knowledge_base.preprocessing.constants import (
    DEFAULT_CRAWL_DEPTH,
    DEFAULT_WEB_FILTERS,
    DEFAULT_WEB_CRAWL_LIMIT,
)
from mindsdb.interfaces.knowledge_base.preprocessing.document_loader import DocumentLoader
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities import log
from mindsdb.utilities.exception import EntityNotExistsError, EntityExistsError


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
        except EntityExistsError as e:
            return http_error(HTTPStatus.BAD_REQUEST, "Knowledge base already exists", str(e))

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
            mysql_proxy = FakeMysqlProxy()

            # Initialize DocumentLoader with required components
            document_loader = DocumentLoader(
                file_controller=file_controller,
                file_splitter=file_splitter,
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

            # update KB
            update_kb_data = {}
            if "params" in kb_data:
                allowed_keys = [
                    "id_column",
                    "metadata_columns",
                    "content_columns",
                    "preprocessing",
                    "reranking_model",
                    "embedding_model",
                ]
                update_kb_data = {k: v for k, v in kb_data["params"].items() if k in allowed_keys}
            if update_kb_data or "preprocessing" in kb_data:
                session.kb_controller.update(
                    knowledge_base_name,
                    project.name,
                    params=update_kb_data,
                    preprocessing_config=kb_data.get("preprocessing"),
                )

        except ExecutorException as e:
            logger.exception("Error during preprocessing and insertion:")
            return http_error(
                HTTPStatus.BAD_REQUEST,
                "Invalid SELECT query",
                f'Executing "query" failed. Needs to be a valid SELECT statement that returns data: {e}',
            )

        except Exception as e:
            logger.exception("Error during preprocessing and insertion:")
            return http_error(
                HTTPStatus.BAD_REQUEST, "Preprocessing Error", f"Error during preprocessing and insertion: {e}"
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
