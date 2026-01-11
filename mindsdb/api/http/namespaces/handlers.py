import os
import tempfile
import importlib
import multipart
from pathlib import Path
from http import HTTPStatus

from flask import request, send_file, current_app as ca
from flask_restx import Resource

from mindsdb_sql_parser.ast import Identifier
from mindsdb_sql_parser.ast.mindsdb import CreateMLEngine

from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.integrations.libs.ml_exec_base import process_cache
from mindsdb.integrations.utilities.install import install_dependencies
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.handlers import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.utilities.exception import EntityExistsError
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def _resolve_handler_readme_path(handler_folder: str) -> Path:
    handler_folder_name = Path(handler_folder).name
    if handler_folder_name != handler_folder or ".." in handler_folder:
        raise ValueError(f"Handler folder '{handler_folder}' is invalid.")

    mindsdb_path = Path(importlib.util.find_spec("mindsdb").origin).parent
    base_handlers_path = mindsdb_path.joinpath("integrations/handlers").resolve()
    readme_path = base_handlers_path.joinpath(handler_folder_name).joinpath("README.md").resolve()

    if base_handlers_path not in readme_path.parents:
        raise ValueError(f"Handler folder '{handler_folder}' is invalid.")

    return readme_path


@ns_conf.route("/")
class HandlersList(Resource):
    @ns_conf.doc("handlers_list")
    @api_endpoint_metrics("GET", "/handlers")
    def get(self):
        """List all db handlers"""

        if request.args.get("lazy") == "1":
            handlers = ca.integration_controller.get_handlers_metadata()
        else:
            handlers = ca.integration_controller.get_handlers_import_status()
        result = []
        for handler_type, handler_meta in handlers.items():
            # remove non-integration handlers
            if handler_type not in ["utilities", "dummy_data"]:
                row = {"name": handler_type}
                row.update(handler_meta)
                del row["path"]
                result.append(row)
        return result


@ns_conf.route("/<handler_name>/icon")
class HandlerIcon(Resource):
    @ns_conf.param("handler_name", "Handler name")
    @api_endpoint_metrics("GET", "/handlers/handler/icon")
    def get(self, handler_name):
        try:
            handler_meta = ca.integration_controller.get_handlers_metadata().get(handler_name)
            if handler_meta is None:
                return http_error(HTTPStatus.NOT_FOUND, "Icon not found", f"Icon for {handler_name} not found")
            icon_name = handler_meta["icon"]["name"]
            handler_folder = handler_meta["import"]["folder"]
            mindsdb_path = Path(importlib.util.find_spec("mindsdb").origin).parent
            icon_path = mindsdb_path.joinpath("integrations/handlers").joinpath(handler_folder).joinpath(icon_name)
            if icon_path.is_absolute() is False:
                icon_path = Path(os.getcwd()).joinpath(icon_path)
        except Exception:
            error_message = f"Icon for '{handler_name}' not found"
            logger.warning(error_message)
            return http_error(HTTPStatus.NOT_FOUND, "Icon not found", error_message)
        else:
            return send_file(icon_path)


@ns_conf.route("/<handler_name>")
class HandlerInfo(Resource):
    @ns_conf.param("handler_name", "Handler name")
    @api_endpoint_metrics("GET", "/handlers/handler")
    def get(self, handler_name):
        handler_meta = ca.integration_controller.get_handler_meta(handler_name)
        row = {"name": handler_name}
        row.update(handler_meta)
        del row["path"]
        del row["icon"]
        return row


@ns_conf.route("/<handler_name>/readme")
class HandlerReadme(Resource):
    @ns_conf.param("handler_name", "Handler name")
    @api_endpoint_metrics("GET", "/handlers/handler/readme")
    def get(self, handler_name):
        try:
            handler_meta = ca.integration_controller.get_handler_meta(handler_name)
        except Exception:
            return http_error(
                HTTPStatus.NOT_FOUND,
                "Readme not found",
                f"Handler '{handler_name}' not found",
            )

        def make_response(*, error_message=None, readme=None):
            return {"name": handler_name, "readme": readme, "error_message": error_message}

        if handler_meta is None:
            error_message = f"Handler '{handler_name}' not found"
            logger.warning(error_message)
            return make_response(error_message=error_message)

        handler_folder = handler_meta.get("import", {}).get("folder")
        if handler_folder is None:
            error_message = f"Handler '{handler_name}' does not define a folder"
            logger.warning(error_message)
            return make_response(error_message=error_message)

        try:
            readme_path = _resolve_handler_readme_path(handler_folder)
        except ValueError as exc:
            error_message = str(exc)
            logger.warning(error_message)
            return make_response(error_message=error_message)

        try:
            with open(readme_path, "r", encoding="utf-8") as readme_file:
                readme_content = readme_file.read()
        except FileNotFoundError:
            error_message = f"README.md for handler '{handler_name}' not found"
            logger.warning(error_message)
            return make_response(error_message=error_message)

        return make_response(readme=readme_content)


@ns_conf.route("/<handler_name>/install")
class InstallDependencies(Resource):
    @ns_conf.param("handler_name", "Handler name")
    @api_endpoint_metrics("POST", "/handlers/handler/install")
    def post(self, handler_name):
        handler_meta = ca.integration_controller.get_handler_meta(handler_name)

        if handler_meta is None:
            return f"Unknown handler: {handler_name}", 400

        if handler_meta.get("import", {}).get("success", False) is True:
            return "Installed", 200

        dependencies = handler_meta["import"]["dependencies"]
        if len(dependencies) == 0:
            return "Installed", 200

        result = install_dependencies(dependencies)

        # reload it if any result, so we can get new error message
        ca.integration_controller.reload_handler_module(handler_name)
        if result.get("success") is True:
            # If warm processes are available in the cache, remove them.
            # This will force a new process to be created with the installed dependencies.
            process_cache.remove_processes_for_handler(handler_name)
            return "", 200
        return http_error(
            500,
            f"Failed to install dependencies for {handler_meta.get('title', handler_name)}",
            result.get("error_message", "unknown error"),
        )


def prepare_formdata():
    params = {}
    file_names = []

    def on_field(field):
        name = field.field_name.decode()
        value = field.value.decode()
        params[name] = value

    def on_file(file):
        file_name = file.file_name.decode()
        if Path(file_name).name != file_name:
            raise ValueError(f"Wrong file name: {file_name}")

        field_name = file.field_name.decode()
        if field_name not in ("code", "modules"):
            raise ValueError(f"Wrong field name: {field_name}")

        params[field_name] = file.file_object
        file_names.append(field_name)

    temp_dir_path = tempfile.mkdtemp(prefix="mindsdb_file_")

    parser = multipart.create_form_parser(
        headers=request.headers,
        on_field=on_field,
        on_file=on_file,
        config={
            "UPLOAD_DIR": temp_dir_path.encode(),  # bytes required
            "UPLOAD_KEEP_FILENAME": True,
            "UPLOAD_KEEP_EXTENSIONS": True,
            "MAX_MEMORY_FILE_SIZE": float("inf"),
        },
    )

    while True:
        chunk = request.stream.read(8192)
        if not chunk:
            break
        parser.write(chunk)
    parser.finalize()
    parser.close()

    for file_name in file_names:
        file_path = os.path.join(temp_dir_path, file_name)
        with open(file_path, "wb") as f:
            params[file_name].seek(0)
            f.write(params[file_name].read())
        params[file_name].close()
        params[file_name] = file_path

    return params


@ns_conf.route("/byom/<name>")
@ns_conf.param("name", "Name of the model")
class BYOMUpload(Resource):
    @ns_conf.doc("post_file")
    @api_endpoint_metrics("POST", "/handlers/byom/handler")
    def post(self, name):
        params = prepare_formdata()

        code_file_path = params["code"]
        try:
            module_file_path = params["modules"]
        except KeyError:
            module_file_path = Path(code_file_path).parent / "requirements.txt"
            module_file_path.touch()
            module_file_path = str(module_file_path)

        connection_args = {"code": code_file_path, "modules": module_file_path, "type": params.get("type")}

        session = SessionController()

        base_ml_handler = session.integration_controller.get_ml_handler(name)
        base_ml_handler.update_engine(connection_args)

        engine_storage = HandlerStorage(base_ml_handler.integration_id)

        engine_versions = [int(x) for x in engine_storage.get_connection_args()["versions"].keys()]

        return {"last_engine_version": max(engine_versions), "engine_versions": engine_versions}

    @ns_conf.doc("put_file")
    @api_endpoint_metrics("PUT", "/handlers/byom/handler")
    def put(self, name):
        """upload new model
        params in FormData:
            - code
            - modules
        """

        params = prepare_formdata()

        code_file_path = params["code"]
        try:
            module_file_path = params["modules"]
        except KeyError:
            module_file_path = Path(code_file_path).parent / "requirements.txt"
            module_file_path.touch()
            module_file_path = str(module_file_path)

        connection_args = {
            "code": code_file_path,
            "modules": module_file_path,
            "mode": params.get("mode"),
            "type": params.get("type"),
        }

        ast_query = CreateMLEngine(name=Identifier(name), handler="byom", params=connection_args)
        sql_session = SessionController()
        command_executor = ExecuteCommands(sql_session)
        try:
            command_executor.execute_command(ast_query)
        except EntityExistsError:
            return http_error(
                HTTPStatus.CONFLICT,
                "Engine already exists",
                f'Engine "{name}" already exists',
            )

        return "", 200
