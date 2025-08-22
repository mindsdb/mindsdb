import os
import sys
import base64
import shutil
import ast
import time
import tempfile
import importlib
import threading
from pathlib import Path
from copy import deepcopy
from typing import Optional
from textwrap import dedent
from collections import OrderedDict
import inspect

from sqlalchemy import func

from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.interfaces.storage.fs import FsStore, FileStorage, RESOURCE_GROUP
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE, HANDLER_TYPE
from mindsdb.interfaces.model.functions import get_model_records
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.libs.base import BaseHandler
import mindsdb.utilities.profiler as profiler
from mindsdb.interfaces.data_catalog.data_catalog_loader import DataCatalogLoader

logger = log.getLogger(__name__)


class HandlersCache:
    """Cache for data handlers that keep connections opened during ttl time from handler last use"""

    def __init__(self, ttl: int = 60):
        """init cache

        Args:
            ttl (int): time to live (in seconds) for record in cache
        """
        self.ttl = ttl
        self.handlers = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self.cleaner_thread = None

    def __del__(self):
        self._stop_clean()

    def _start_clean(self) -> None:
        """start worker that close connections after ttl expired"""
        if isinstance(self.cleaner_thread, threading.Thread) and self.cleaner_thread.is_alive():
            return
        self._stop_event.clear()
        self.cleaner_thread = threading.Thread(target=self._clean, name="HandlersCache.clean")
        self.cleaner_thread.daemon = True
        self.cleaner_thread.start()

    def _stop_clean(self) -> None:
        """stop clean worker"""
        self._stop_event.set()

    def set(self, handler: DatabaseHandler):
        """add (or replace) handler in cache

        Args:
            handler (DatabaseHandler)
        """
        with self._lock:
            try:
                # If the handler is defined to be thread safe, set 0 as the last element of the key, otherwise set the thrad ID.
                key = (
                    handler.name,
                    ctx.company_id,
                    0 if getattr(handler, "thread_safe", False) else threading.get_native_id(),
                )
                handler.connect()
                self.handlers[key] = {"handler": handler, "expired_at": time.time() + self.ttl}
            except Exception:
                pass
            self._start_clean()

    def get(self, name: str) -> Optional[DatabaseHandler]:
        """get handler from cache by name

        Args:
            name (str): handler name

        Returns:
            DatabaseHandler
        """
        with self._lock:
            # If the handler is not thread safe, the thread ID will be assigned to the last element of the key.
            key = (name, ctx.company_id, threading.get_native_id())
            if key not in self.handlers:
                # If the handler is thread safe, a 0 will be assigned to the last element of the key.
                key = (name, ctx.company_id, 0)
            if key not in self.handlers or self.handlers[key]["expired_at"] < time.time():
                return None
            self.handlers[key]["expired_at"] = time.time() + self.ttl
            return self.handlers[key]["handler"]

    def delete(self, name: str) -> None:
        """delete handler from cache

        Args:
            name (str): handler name
        """
        with self._lock:
            key = (name, ctx.company_id, threading.get_native_id())
            if key in self.handlers:
                try:
                    self.handlers[key].disconnect()
                except Exception:
                    pass
                del self.handlers[key]
            if len(self.handlers) == 0:
                self._stop_clean()

    def _clean(self) -> None:
        """worker that delete from cache handlers that was not in use for ttl"""
        while self._stop_event.wait(timeout=3) is False:
            with self._lock:
                for key in list(self.handlers.keys()):
                    if (
                        self.handlers[key]["expired_at"] < time.time()
                        and sys.getrefcount(self.handlers[key]) == 2  # returned ref count is always 1 higher
                    ):
                        try:
                            self.handlers[key].disconnect()
                        except Exception:
                            pass
                        del self.handlers[key]
                if len(self.handlers) == 0:
                    self._stop_event.set()


class IntegrationController:
    @staticmethod
    def _is_not_empty_str(s):
        return isinstance(s, str) and len(s) > 0

    def __init__(self):
        self._import_lock = threading.Lock()
        self._load_handler_modules()
        self.handlers_cache = HandlersCache()

    def _add_integration_record(self, name, engine, connection_args):
        integration_record = db.Integration(
            name=name, engine=engine, data=connection_args or {}, company_id=ctx.company_id
        )
        db.session.add(integration_record)
        db.session.commit()
        return integration_record.id

    def add(self, name: str, engine, connection_args):
        logger.debug(
            "%s: add method calling name=%s, engine=%s, connection_args=%s, company_id=%s",
            self.__class__.__name__,
            name,
            engine,
            connection_args,
            ctx.company_id,
        )
        handler_meta = self.get_handler_meta(engine)

        if not name.islower():
            raise ValueError(f"The name must be in lower case: {name}")

        accept_connection_args = handler_meta.get("connection_args")
        logger.debug("%s: accept_connection_args - %s", self.__class__.__name__, accept_connection_args)

        files_dir = None
        if accept_connection_args is not None and connection_args is not None:
            for arg_name, arg_value in connection_args.items():
                if arg_name in accept_connection_args and accept_connection_args[arg_name]["type"] == ARG_TYPE.PATH:
                    if files_dir is None:
                        files_dir = tempfile.mkdtemp(prefix="mindsdb_files_")
                    shutil.copy(arg_value, files_dir)
                    connection_args[arg_name] = Path(arg_value).name

        integration_id = self._add_integration_record(name, engine, connection_args)

        if files_dir is not None:
            store = FileStorage(resource_group=RESOURCE_GROUP.INTEGRATION, resource_id=integration_id, sync=False)
            store.add(files_dir, "")
            store.push()

        if handler_meta.get("type") == HANDLER_TYPE.ML:
            ml_handler = self.get_ml_handler(name)
            ml_handler.create_engine(connection_args, integration_id)

        return integration_id

    def modify(self, name, data):
        self.handlers_cache.delete(name)
        integration_record = self._get_integration_record(name)
        if isinstance(integration_record.data, dict) and integration_record.data.get("is_demo") is True:
            raise ValueError("It is forbidden to change properties of the demo object")
        old_data = deepcopy(integration_record.data)
        for k in old_data:
            if k not in data:
                data[k] = old_data[k]

        integration_record.data = data
        db.session.commit()

    def delete(self, name: str, strict_case: bool = False) -> None:
        """Delete an integration by name.

        Args:
            name (str): The name of the integration to delete.
            strict_case (bool, optional): If True, the integration name is case-sensitive. Defaults to False.

        Raises:
            Exception: If the integration cannot be deleted (system, permanent, demo, in use, or has active models).

        Returns:
            None
        """
        if name == "files":
            raise Exception("Unable to drop: is system database")

        self.handlers_cache.delete(name)

        # check permanent integration
        if name.lower() in self.handler_modules:
            handler = self.handler_modules[name]

            if getattr(handler, "permanent", False) is True:
                raise Exception("Unable to drop permanent integration")

        integration_record = self._get_integration_record(name, case_sensitive=strict_case)
        if isinstance(integration_record.data, dict) and integration_record.data.get("is_demo") is True:
            raise Exception("Unable to drop demo object")

        # if this is ml engine
        engine_models = get_model_records(ml_handler_name=name, deleted_at=None)
        active_models = [m.name for m in engine_models if m.deleted_at is None]
        if len(active_models) > 0:
            raise Exception(f"Unable to drop ml engine with active models: {active_models}")

        # check linked KBs
        kb = db.KnowledgeBase.query.filter_by(vector_database_id=integration_record.id).first()
        if kb is not None:
            raise Exception(f"Unable to drop, integration is used by knowledge base: {kb.name}")

        # check linked predictors
        models = get_model_records()
        for model in models:
            if (
                model.data_integration_ref is not None
                and model.data_integration_ref.get("type") == "integration"
                and isinstance(model.data_integration_ref.get("id"), int)
                and model.data_integration_ref["id"] == integration_record.id
            ):
                model.data_integration_ref = None

        # unlink deleted models
        for model in engine_models:
            if model.deleted_at is not None:
                model.integration_id = None

        # Remove the integration metadata from the data catalog (if enabled).
        # TODO: Can this be handled via cascading delete in the database?
        if self.get_handler_meta(integration_record.engine).get("type") == HANDLER_TYPE.DATA and Config().get(
            "data_catalog", {}
        ).get("enabled", False):
            data_catalog_reader = DataCatalogLoader(database_name=name)
            data_catalog_reader.unload_metadata()

        db.session.delete(integration_record)
        db.session.commit()

    def _get_integration_record_data(self, integration_record, show_secrets=True):
        if (
            integration_record is None
            or integration_record.data is None
            or isinstance(integration_record.data, dict) is False
        ):
            return None
        data = deepcopy(integration_record.data)

        bundle_path = data.get("secure_connect_bundle")
        mysql_ssl_ca = data.get("ssl_ca")
        mysql_ssl_cert = data.get("ssl_cert")
        mysql_ssl_key = data.get("ssl_key")
        if (
            data.get("type") in ("mysql", "mariadb")
            and (
                self._is_not_empty_str(mysql_ssl_ca)
                or self._is_not_empty_str(mysql_ssl_cert)
                or self._is_not_empty_str(mysql_ssl_key)
            )
            or data.get("type") in ("cassandra", "scylla")
            and bundle_path is not None
        ):
            fs_store = FsStore()
            integrations_dir = Config()["paths"]["integrations"]
            folder_name = f"integration_files_{integration_record.company_id}_{integration_record.id}"
            fs_store.get(folder_name, base_dir=integrations_dir)

        handler_meta = self.get_handler_metadata(integration_record.engine)
        integration_type = None
        if isinstance(handler_meta, dict):
            # in other cases, the handler directory is likely not exist.
            integration_type = handler_meta.get("type")

        if show_secrets is False and handler_meta is not None:
            connection_args = handler_meta.get("connection_args", None)
            if isinstance(connection_args, dict):
                if integration_type == HANDLER_TYPE.DATA:
                    for key, value in connection_args.items():
                        if key in data and value.get("secret", False) is True:
                            data[key] = "******"
                elif integration_type == HANDLER_TYPE.ML:
                    creation_args = connection_args.get("creation_args")
                    if isinstance(creation_args, dict):
                        for key, value in creation_args.items():
                            if key in data and value.get("secret", False) is True:
                                data[key] = "******"
                else:
                    raise ValueError(f"Unexpected handler type: {integration_type}")
            else:
                # region obsolete, del in future
                if "password" in data:
                    data["password"] = None
                if (
                    data.get("type") == "redis"
                    and isinstance(data.get("connection"), dict)
                    and "password" in data["connection"]
                ):
                    data["connection"] = None
                # endregion

        class_type, permanent = None, False
        if handler_meta is not None:
            class_type = handler_meta.get("class_type")
            permanent = handler_meta.get("permanent", False)

        return {
            "id": integration_record.id,
            "name": integration_record.name,
            "type": integration_type,
            "class_type": class_type,
            "engine": integration_record.engine,
            "permanent": permanent,
            "date_last_update": deepcopy(integration_record.updated_at),  # to del ?
            "connection_data": data,
        }

    def get_by_id(self, integration_id, show_secrets=True):
        integration_record = (
            db.session.query(db.Integration).filter_by(company_id=ctx.company_id, id=integration_id).first()
        )
        return self._get_integration_record_data(integration_record, show_secrets)

    def get(self, name, show_secrets=True, case_sensitive=False):
        try:
            integration_record = self._get_integration_record(name, case_sensitive)
        except EntityNotExistsError:
            return None
        return self._get_integration_record_data(integration_record, show_secrets)

    @staticmethod
    def _get_integration_record(name: str, case_sensitive: bool = False) -> db.Integration:
        """Get integration record by name

        Args:
            name (str): name of the integration
            case_sensitive (bool): should search be case sensitive or not

        Retruns:
            db.Integration
        """
        if case_sensitive:
            integration_records = db.session.query(db.Integration).filter_by(company_id=ctx.company_id, name=name).all()
            if len(integration_records) > 1:
                raise Exception(f"There is {len(integration_records)} integrations with name '{name}'")
            if len(integration_records) == 0:
                raise EntityNotExistsError(f"There is no integration with name '{name}'")
            integration_record = integration_records[0]
        else:
            integration_record = (
                db.session.query(db.Integration)
                .filter(
                    (db.Integration.company_id == ctx.company_id)
                    & (func.lower(db.Integration.name) == func.lower(name))
                )
                .first()
            )
            if integration_record is None:
                raise EntityNotExistsError(f"There is no integration with name '{name}'")

        return integration_record

    def get_all(self, show_secrets=True):
        integration_records = db.session.query(db.Integration).filter_by(company_id=ctx.company_id).all()
        integration_dict = {}
        for record in integration_records:
            if record is None or record.data is None:
                continue
            integration_dict[record.name] = self._get_integration_record_data(record, show_secrets)
        return integration_dict

    def _make_handler_args(
        self,
        name: str,
        handler_type: str,
        connection_data: dict,
        integration_id: int = None,
        file_storage: FileStorage = None,
        handler_storage: HandlerStorage = None,
    ):
        handler_args = dict(
            name=name,
            integration_id=integration_id,
            connection_data=connection_data,
            file_storage=file_storage,
            handler_storage=handler_storage,
        )

        if handler_type == "files":
            handler_args["file_controller"] = FileController()
        elif self.handler_modules.get(handler_type, False).type == HANDLER_TYPE.ML:
            handler_args["handler_controller"] = self
            handler_args["company_id"] = ctx.company_id

        return handler_args

    def create_tmp_handler(self, name: str, engine: str, connection_args: dict) -> dict:
        """Create temporary handler, mostly for testing connections

        Args:
            name (str): Integration  name
            engine (str): Integration engine name
            connection_args (dict): Connection arguments

        Returns:
            HandlerClass: Handler class instance
        """
        integration_id = int(time.time() * 10000)

        file_storage = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION, resource_id=integration_id, root_dir="tmp", sync=False
        )
        handler_storage = HandlerStorage(integration_id, root_dir="tmp", is_temporal=True)

        handler_meta = self.get_handler_meta(engine)
        if handler_meta is None:
            raise ImportError(f"Handler '{engine}' does not exist")
        if handler_meta["import"]["success"] is False:
            raise ImportError(f"Handler '{engine}' cannot be imported: {handler_meta['import']['error_message']}")
        HandlerClass = self.handler_modules[engine].Handler

        handler_args = self._make_handler_args(
            name=name,
            handler_type=engine,
            connection_data=connection_args,
            integration_id=integration_id,
            file_storage=file_storage,
            handler_storage=handler_storage,
        )
        handler = HandlerClass(**handler_args)
        return handler

    def copy_integration_storage(self, integration_id_from, integration_id_to):
        storage_from = HandlerStorage(integration_id_from)
        root_path = ""

        if storage_from.is_empty():
            return None
        folder_from = storage_from.folder_get(root_path)

        storage_to = HandlerStorage(integration_id_to)
        folder_to = storage_to.folder_get(root_path)

        shutil.copytree(folder_from, folder_to, dirs_exist_ok=True)
        storage_to.folder_sync(root_path)

    def get_ml_handler(self, name: str, case_sensitive: bool = False) -> BaseMLEngine:
        """Get ML handler by name
        Args:
            name (str): name of the handler
            case_sensitive (bool): should case be taken into account when searching by name

        Returns:
            BaseMLEngine
        """
        integration_record = self._get_integration_record(name, case_sensitive)
        integration_engine = integration_record.engine

        integration_meta = self.get_handler_meta(integration_engine)
        if integration_meta is None:
            raise Exception(f"Handler '{name}' does not exists")

        if integration_meta.get("type") != HANDLER_TYPE.ML:
            raise Exception(f"Handler '{name}' must be ML type")

        logger.info(
            f"{self.__class__.__name__}.get_handler: create a ML client "
            + f"{integration_record.name}/{integration_record.id}"
        )
        handler = BaseMLEngineExec(
            name=integration_record.name,
            integration_id=integration_record.id,
            handler_module=self.handler_modules[integration_engine],
        )

        return handler

    @profiler.profile()
    def get_data_handler(self, name: str, case_sensitive: bool = False, connect=True) -> BaseHandler:
        """Get DATA handler (DB or API) by name

        Args:
            name (str): name of the handler
            case_sensitive (bool): should case be taken into account when searching by name

        Returns:
            BaseHandler: data handler
        """
        handler = self.handlers_cache.get(name)
        if handler is not None:
            return handler

        integration_record = self._get_integration_record(name, case_sensitive)
        integration_engine = integration_record.engine

        integration_meta = self.get_handler_meta(integration_engine)

        if integration_meta is None:
            raise Exception(f"Handler '{name}' does not exist")

        if integration_meta.get("type") != HANDLER_TYPE.DATA:
            raise Exception(f"Handler '{name}' must be DATA type")

        integration_data = self._get_integration_record_data(integration_record, True)
        if integration_data is None:
            raise Exception(f"Can't find integration_record for handler '{name}'")
        connection_data = integration_data.get("connection_data", {})
        logger.debug(
            "%s.get_handler: connection_data=%s, engine=%s",
            self.__class__.__name__,
            connection_data,
            integration_engine,
        )

        if integration_meta["import"]["success"] is False:
            msg = dedent(f"""\
                Handler '{integration_engine}' cannot be used. Reason is:
                    {integration_meta["import"]["error_message"]}
            """)
            is_cloud = Config().get("cloud", False)
            if is_cloud is False:
                msg += dedent(f"""

                If error is related to missing dependencies, then try to run command in shell and restart mindsdb:
                    pip install mindsdb[{integration_engine}]
                """)
            logger.debug(msg)
            raise Exception(msg)

        connection_args = integration_meta.get("connection_args")
        logger.debug("%s.get_handler: connection args - %s", self.__class__.__name__, connection_args)

        file_storage = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            resource_id=integration_record.id,
            sync=True,
        )
        handler_storage = HandlerStorage(integration_record.id)

        if isinstance(connection_args, (dict, OrderedDict)):
            files_to_get = {
                arg_name: arg_value
                for arg_name, arg_value in connection_data.items()
                if arg_name in connection_args and connection_args.get(arg_name)["type"] == ARG_TYPE.PATH
            }
            if len(files_to_get) > 0:
                for file_name, file_path in files_to_get.items():
                    connection_data[file_name] = file_storage.get_path(file_path)

        handler_ars = self._make_handler_args(
            name=name,
            handler_type=integration_engine,
            connection_data=connection_data,
            integration_id=integration_data["id"],
            file_storage=file_storage,
            handler_storage=handler_storage,
        )

        HandlerClass = self.handler_modules[integration_engine].Handler
        handler = HandlerClass(**handler_ars)
        if connect:
            self.handlers_cache.set(handler)

        return handler

    def reload_handler_module(self, handler_name):
        importlib.reload(self.handler_modules[handler_name])
        try:
            handler_meta = self._get_handler_meta(handler_name)
        except Exception as e:
            handler_meta = self.handlers_import_status[handler_name]
            handler_meta["import"]["success"] = False
            handler_meta["import"]["error_message"] = str(e)

        self.handlers_import_status[handler_name] = handler_meta

    def _read_dependencies(self, path):
        dependencies = []
        requirements_txt = Path(path).joinpath("requirements.txt")
        if requirements_txt.is_file():
            with open(str(requirements_txt), "rt") as f:
                dependencies = [x.strip(" \t\n") for x in f.readlines()]
                dependencies = [x for x in dependencies if len(x) > 0]
        return dependencies

    def _get_handler_meta(self, handler_name):
        module = self.handler_modules[handler_name]

        handler_dir = Path(module.__path__[0])
        handler_folder_name = handler_dir.name

        import_error = getattr(module, "import_error", None)
        handler_meta = self.handlers_import_status[handler_name]
        handler_meta["import"]["success"] = import_error is None
        handler_meta["version"] = module.version
        handler_meta["thread_safe"] = getattr(module, "thread_safe", False)

        if import_error is not None:
            handler_meta["import"]["error_message"] = str(import_error)

        handler_type = getattr(module, "type", None)
        handler_class = None
        if hasattr(module, "Handler") and inspect.isclass(module.Handler):
            handler_class = module.Handler
            if issubclass(handler_class, BaseMLEngine):
                handler_meta["class_type"] = "ml"
            elif issubclass(handler_class, DatabaseHandler):
                handler_meta["class_type"] = "sql"
            if issubclass(handler_class, APIHandler):
                handler_meta["class_type"] = "api"

        if handler_type == HANDLER_TYPE.ML:
            # for ml engines, patch the connection_args from the argument probing
            if handler_class:
                try:
                    prediction_args = handler_class.prediction_args()
                    creation_args = getattr(module, "creation_args", handler_class.creation_args())
                    connection_args = {"prediction": prediction_args, "creation_args": creation_args}
                    setattr(module, "connection_args", connection_args)
                    logger.debug("Patched connection_args for %s", handler_folder_name)
                except Exception as e:
                    # do nothing
                    logger.debug("Failed to patch connection_args for %s, reason: %s", handler_folder_name, str(e))

        module_attrs = [
            attr
            for attr in ["connection_args_example", "connection_args", "description", "type", "title"]
            if hasattr(module, attr)
        ]

        for attr in module_attrs:
            handler_meta[attr] = getattr(module, attr)

        # endregion
        if hasattr(module, "permanent"):
            handler_meta["permanent"] = module.permanent
        else:
            if handler_meta.get("name") in ("files", "views", "lightwood"):
                handler_meta["permanent"] = True
            else:
                handler_meta["permanent"] = False

        return handler_meta

    def _get_handler_icon(self, handler_dir, icon_path):
        icon = {}
        try:
            icon_path = handler_dir.joinpath(icon_path)
            icon_type = icon_path.name[icon_path.name.rfind(".") + 1 :].lower()

            if icon_type == "svg":
                with open(str(icon_path), "rt") as f:
                    icon["data"] = f.read()
            else:
                with open(str(icon_path), "rb") as f:
                    icon["data"] = base64.b64encode(f.read()).decode("utf-8")

            icon["name"] = icon_path.name
            icon["type"] = icon_type

        except Exception as e:
            logger.error(f"Error reading icon for {handler_dir}, {e}!")
        return icon

    def _load_handler_modules(self):
        mindsdb_path = Path(importlib.util.find_spec("mindsdb").origin).parent
        handlers_path = mindsdb_path.joinpath("integrations/handlers")

        # edge case: running from tests directory, find_spec finds the base folder instead of actual package
        if not os.path.isdir(handlers_path):
            mindsdb_path = Path(importlib.util.find_spec("mindsdb").origin).parent.joinpath("mindsdb")
            handlers_path = mindsdb_path.joinpath("integrations/handlers")

        self.handler_modules = {}
        self.handlers_import_status = {}
        for handler_dir in handlers_path.iterdir():
            if handler_dir.is_dir() is False or handler_dir.name.startswith("__"):
                continue

            handler_info = self._get_handler_info(handler_dir)
            if "name" not in handler_info:
                continue
            handler_name = handler_info["name"]
            dependencies = self._read_dependencies(handler_dir)
            handler_meta = {
                "path": handler_dir,
                "import": {
                    "success": None,
                    "error_message": None,
                    "folder": handler_dir.name,
                    "dependencies": dependencies,
                },
                "name": handler_name,
                "permanent": handler_info.get("permanent", False),
                "connection_args": handler_info.get("connection_args", None),
                "class_type": handler_info.get("class_type", None),
                "type": handler_info.get("type"),
            }
            if "icon_path" in handler_info:
                icon = self._get_handler_icon(handler_dir, handler_info["icon_path"])
                if icon:
                    handler_meta["icon"] = icon
            self.handlers_import_status[handler_name] = handler_meta

    def _get_connection_args(self, args_file: Path, param_name: str) -> dict:
        """
        Extract connection args dict from connection args file of a handler

        :param args_file: path to file with connection args
        :param param_name: the name of variable which contains connection args
        :return: extracted connection arguments
        """

        code = ast.parse(args_file.read_text())

        args = {}
        for item in code.body:
            if not isinstance(item, ast.Assign):
                continue
            if not item.targets[0].id == param_name:
                continue
            if hasattr(item.value, "keywords"):
                for keyword in item.value.keywords:
                    name = keyword.arg
                    params = keyword.value
                    if isinstance(params, ast.Dict):
                        # get dict value
                        info = {}
                        for i, k in enumerate(params.keys):
                            if not isinstance(k, ast.Constant):
                                continue
                            v = params.values[i]
                            if isinstance(v, ast.Constant):
                                v = v.value
                            elif isinstance(v, ast.Attribute):
                                # assume it is ARG_TYPE
                                v = getattr(ARG_TYPE, v.attr, None)
                            else:
                                v = None
                            info[k.value] = v
                    args[name] = info
        return args

    def _get_base_class_type(self, code, handler_dir: Path) -> Optional[str]:
        """
        Find base class of data handler: sql or api
        It tries to find import inside try-except of init file
        They parsed this import in order to find base class of data handler

        :param code: parsed code of __init__ file of a handler
        :param handler_dir: folder of a handler
        :return: base class type
        """

        module_file = None
        for block in code.body:
            if not isinstance(block, ast.Try):
                continue
            for item in block.body:
                if isinstance(item, ast.ImportFrom):
                    module_file = item.module
                    break
        if module_file is None:
            return

        path = handler_dir / f"{module_file}.py"

        if not path.exists():
            return
        code = ast.parse(path.read_text())
        # find base class of handler.
        #  TODO trace inheritance (is used only for sql handler)
        for item in code.body:
            if isinstance(item, ast.ClassDef):
                bases = [base.id for base in item.bases]
                if "APIHandler" in bases or "MetaAPIHandler" in bases:
                    return "api"
        return "sql"

    def _get_handler_info(self, handler_dir: Path) -> dict:
        """
        Get handler info without importing it
        :param handler_dir: folder of handler
        :return: Extracted params:
          - defined constants in init file
          - connection arguments
        """

        init_file = handler_dir / "__init__.py"
        if not init_file.exists():
            return {}
        code = ast.parse(init_file.read_text())

        info = {}
        for item in code.body:
            if not isinstance(item, ast.Assign):
                continue
            if isinstance(item.targets[0], ast.Name):
                name = item.targets[0].id
                if isinstance(item.value, ast.Constant):
                    info[name] = item.value.value
                if isinstance(item.value, ast.Attribute) and name == "type":
                    if item.value.attr == "ML":
                        info[name] = HANDLER_TYPE.ML
                        info["class_type"] = "ml"
                    else:
                        info[name] = HANDLER_TYPE.DATA
                        info["class_type"] = self._get_base_class_type(code, handler_dir) or "sql"

        # connection args
        if info["type"] == HANDLER_TYPE.ML:
            args_file = handler_dir / "creation_args.py"
            if args_file.exists():
                info["connection_args"] = {
                    "prediction": {},
                    "creation_args": self._get_connection_args(args_file, "creation_args"),
                }
        else:
            args_file = handler_dir / "connection_args.py"
            if args_file.exists():
                info["connection_args"] = self._get_connection_args(args_file, "connection_args")

        return info

    def import_handler(self, handler_name: str, base_import: str = None):
        with self._import_lock:
            handler_meta = self.handlers_import_status[handler_name]
            handler_dir = handler_meta["path"]

            handler_folder_name = str(handler_dir.name)
            if base_import is None:
                base_import = "mindsdb.integrations.handlers."

            try:
                handler_module = importlib.import_module(f"{base_import}{handler_folder_name}")
                self.handler_modules[handler_name] = handler_module
                handler_meta = self._get_handler_meta(handler_name)
            except Exception as e:
                handler_meta["import"]["success"] = False
                handler_meta["import"]["error_message"] = str(e)

            self.handlers_import_status[handler_meta["name"]] = handler_meta
            return handler_meta

    def get_handlers_import_status(self):
        # tries to import all not imported yet

        result = {}
        for handler_name in list(self.handlers_import_status.keys()):
            handler_meta = self.get_handler_meta(handler_name)
            result[handler_name] = handler_meta

        return result

    def get_handlers_metadata(self):
        return self.handlers_import_status

    def get_handler_metadata(self, handler_name):
        # returns metadata
        return self.handlers_import_status.get(handler_name)

    def get_handler_meta(self, handler_name):
        # returns metadata and tries to import it
        handler_meta = self.handlers_import_status.get(handler_name)
        if handler_meta is None:
            return
        if handler_meta["import"]["success"] is None:
            handler_meta = self.import_handler(handler_name)
        return handler_meta

    def get_handler_module(self, handler_name):
        handler_meta = self.get_handler_meta(handler_name)
        if handler_meta is None:
            return
        if handler_meta["import"]["success"]:
            return self.handler_modules[handler_name]

    def create_permanent_integrations(self):
        for (
            integration_name,
            handler,
        ) in self.get_handlers_metadata().items():
            if handler.get("permanent"):
                integration_meta = integration_controller.get(name=integration_name)
                if integration_meta is None:
                    integration_record = db.Integration(
                        name=integration_name,
                        data={},
                        engine=integration_name,
                        company_id=None,
                    )
                    db.session.add(integration_record)
        db.session.commit()


integration_controller = IntegrationController()
