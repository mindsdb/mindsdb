import os
import sys
import base64
import shutil
import tempfile
import importlib
import threading
import inspect
import multiprocessing
from time import time
from pathlib import Path
from copy import deepcopy
from typing import Optional
from textwrap import dedent
from collections import OrderedDict

from sqlalchemy import func

from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore, FileStorage, FileStorageFactory, RESOURCE_GROUP
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE, HANDLER_TYPE
from mindsdb.integrations.handlers_client.db_client_factory import DBClient
from mindsdb.interfaces.model.functions import get_model_records
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
import mindsdb.utilities.profiler as profiler

logger = get_log()


class HandlersCache:
    """ Cache for data handlers that keep connections opened during ttl time from handler last use
    """

    def __init__(self, ttl: int = 60):
        """ init cache

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
        """ start worker that close connections after ttl expired
        """
        if (
            isinstance(self.cleaner_thread, threading.Thread)
            and self.cleaner_thread.is_alive()
        ):
            return
        self._stop_event.clear()
        self.cleaner_thread = threading.Thread(target=self._clean)
        self.cleaner_thread.daemon = True
        self.cleaner_thread.start()

    def _stop_clean(self) -> None:
        """ stop clean worker
        """
        self._stop_event.set()

    def set(self, handler: DatabaseHandler):
        """ add (or replace) handler in cache

            Args:
                handler (DatabaseHandler)
        """
        # do not cache connections in handlers processes
        if multiprocessing.current_process().name.startswith('HandlerProcess'):
            return
        with self._lock:
            try:
                key = (handler.name, ctx.company_id, threading.get_native_id())
                handler.connect()
                self.handlers[key] = {
                    'handler': handler,
                    'expired_at': time() + self.ttl
                }
            except Exception:
                pass
            self._start_clean()

    def get(self, name: str) -> Optional[DatabaseHandler]:
        """ get handler from cache by name

            Args:
                name (str): handler name

            Returns:
                DatabaseHandler
        """
        with self._lock:
            key = (name, ctx.company_id, threading.get_native_id())
            if (
                key not in self.handlers
                or self.handlers[key]['expired_at'] < time()
            ):
                return None
            self.handlers[key]['expired_at'] = time() + self.ttl
            return self.handlers[key]['handler']

    def delete(self, name: str) -> None:
        """ delete handler from cache

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
        """ worker that delete from cache handlers that was not in use for ttl
        """
        while self._stop_event.wait(timeout=3) is False:
            with self._lock:
                for key in list(self.handlers.keys()):
                    if (
                        self.handlers[key]['expired_at'] < time()
                        and sys.getrefcount(self.handlers[key]) == 2    # returned ref count is always 1 higher
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
        self._load_handler_modules()
        self.handlers_cache = HandlersCache()

    def _add_integration_record(self, name, engine, connection_args):
        integration_record = db.Integration(
            name=name,
            engine=engine,
            data=connection_args or {},
            company_id=ctx.company_id
        )
        db.session.add(integration_record)
        db.session.commit()
        return integration_record.id

    def add(self, name, engine, connection_args):

        logger.debug(
            "%s: add method calling name=%s, engine=%s, connection_args=%s, company_id=%s",
            self.__class__.__name__, name, engine, connection_args, ctx.company_id
        )
        handlers_meta = self.get_handlers_import_status()
        handler_meta = handlers_meta[engine]
        accept_connection_args = handler_meta.get('connection_args')
        logger.debug("%s: accept_connection_args - %s", self.__class__.__name__, accept_connection_args)

        files_dir = None
        if accept_connection_args is not None:
            for arg_name, arg_value in connection_args.items():
                if (
                    arg_name in accept_connection_args
                    and accept_connection_args[arg_name]['type'] == ARG_TYPE.PATH
                ):
                    if files_dir is None:
                        files_dir = tempfile.mkdtemp(prefix='mindsdb_files_')
                    shutil.copy(arg_value, files_dir)
                    connection_args[arg_name] = Path(arg_value).name

        integration_id = self._add_integration_record(name, engine, connection_args)

        if files_dir is not None:
            store = FileStorage(
                resource_group=RESOURCE_GROUP.INTEGRATION,
                resource_id=integration_id,
                sync=False
            )
            store.add(files_dir, '')
            store.push()

        return integration_id

    def modify(self, name, data):
        self.handlers_cache.delete(name)
        integration_record = db.session.query(db.Integration).filter_by(
            company_id=ctx.company_id, name=name
        ).first()
        old_data = deepcopy(integration_record.data)
        for k in old_data:
            if k not in data:
                data[k] = old_data[k]

        integration_record.data = data
        db.session.commit()

    def delete(self, name):
        if name in ('files', 'lightwood'):
            raise Exception('Unable to drop: is system database')

        self.handlers_cache.delete(name)

        # check permanent integration
        if name in self.handler_modules:
            handler = self.handler_modules[name]

            if getattr(handler, 'permanent', False) is True:
                raise Exception('Unable to drop: is permanent integration')

        integration_record = db.session.query(db.Integration).filter_by(company_id=ctx.company_id, name=name).first()

        # if this is ml engine
        engine_models = get_model_records(ml_handler_name=name, deleted_at=None)
        active_models = [m.name for m in engine_models if m.deleted_at is None]
        if len(active_models) > 0:
            raise Exception(f'Unable to drop ml engine with active models: {active_models}')

        # check linked predictors
        models = get_model_records()
        for model in models:
            if (
                model.data_integration_ref is not None
                and model.data_integration_ref.get('type') == 'integration'
                and isinstance(model.data_integration_ref.get('id'), int)
                and model.data_integration_ref['id'] == integration_record.id
            ):
                model.data_integration_ref = None

        # unlink deleted models
        for model in engine_models:
            if model.deleted_at is not None:
                model.integration_id = None

        db.session.delete(integration_record)
        db.session.commit()

    def _get_integration_record_data(self, integration_record, sensitive_info=True):
        if integration_record is None or integration_record.data is None:
            return None
        data = deepcopy(integration_record.data)
        if data.get('password', None) is None:
            data['password'] = ''

        bundle_path = data.get('secure_connect_bundle')
        mysql_ssl_ca = data.get('ssl_ca')
        mysql_ssl_cert = data.get('ssl_cert')
        mysql_ssl_key = data.get('ssl_key')
        if (
            data.get('type') in ('mysql', 'mariadb')
            and (
                self._is_not_empty_str(mysql_ssl_ca)
                or self._is_not_empty_str(mysql_ssl_cert)
                or self._is_not_empty_str(mysql_ssl_key)
            )
            or data.get('type') in ('cassandra', 'scylla')
            and bundle_path is not None
        ):
            fs_store = FsStore()
            integrations_dir = Config()['paths']['integrations']
            folder_name = f'integration_files_{integration_record.company_id}_{integration_record.id}'
            fs_store.get(
                folder_name,
                base_dir=integrations_dir
            )

        if not sensitive_info:
            if 'password' in data:
                data['password'] = None
            if (
                data.get('type') == 'redis'
                and isinstance(data.get('connection'), dict)
                and 'password' in data['connection']
            ):
                data['connection'] = None

        integration_type = None
        integration_module = self.handler_modules.get(integration_record.engine)
        if hasattr(integration_module, 'type'):
            integration_type = integration_module.type

        class_type = None
        if integration_module is not None and inspect.isclass(integration_module.Handler):
            if issubclass(integration_module.Handler, DatabaseHandler):
                class_type = 'sql'
            if issubclass(integration_module.Handler, APIHandler):
                class_type = 'api'
            if issubclass(integration_module.Handler, BaseMLEngine):
                class_type = 'ml'

        return {
            'id': integration_record.id,
            'name': integration_record.name,
            'type': integration_type,
            'class_type': class_type,
            'engine': integration_record.engine,
            'date_last_update': deepcopy(integration_record.updated_at),
            'connection_data': data
        }

    def get_by_id(self, integration_id, sensitive_info=True):
        integration_record = db.session.query(db.Integration).filter_by(company_id=ctx.company_id, id=integration_id).first()
        return self._get_integration_record_data(integration_record, sensitive_info)

    def get(self, name, sensitive_info=True, case_sensitive=False):
        if case_sensitive:
            integration_record = db.session.query(db.Integration).filter_by(
                company_id=ctx.company_id, name=name
            ).first()
        else:
            integration_record = db.session.query(db.Integration).filter(
                (db.Integration.company_id == ctx.company_id)
                & (func.lower(db.Integration.name) == func.lower(name))
            ).first()
        return self._get_integration_record_data(integration_record, sensitive_info)

    def get_all(self, sensitive_info=True):
        integration_records = db.session.query(db.Integration).filter_by(company_id=ctx.company_id).all()
        integration_dict = {}
        for record in integration_records:
            if record is None or record.data is None:
                continue
            integration_dict[record.name] = self._get_integration_record_data(record, sensitive_info)
        return integration_dict

    def check_connections(self):
        connections = {}
        for integration_name, integration_meta in self.get_all().items():
            handler = self.create_tmp_handler(
                handler_type=integration_meta['engine'],
                connection_data=integration_meta['connection_data']
            )
            status = handler.check_connection()
            connections[integration_name] = status.get('success', False)
        return connections

    def _make_handler_args(self, handler_type: str, connection_data: dict, integration_id: int = None):
        handler_ars = dict(
            connection_data=connection_data,
            integration_id=integration_id
        )

        if handler_type == 'files':
            handler_ars['file_controller'] = FileController()
        elif self.handler_modules.get(handler_type, False).type == HANDLER_TYPE.ML:
            handler_ars['handler_controller'] = self
            handler_ars['company_id'] = ctx.company_id

        return handler_ars

    def create_tmp_handler(self, handler_type: str, connection_data: dict) -> object:
        """ Returns temporary handler. That handler does not exists in database.

            Args:
                handler_type (str)
                connection_data (dict)

            Returns:
                Handler object
        """
        handler_meta = self.handlers_import_status[handler_type]
        if not handler_meta["import"]["success"]:
            logger.info(f"to use {handler_type} please install 'pip install mindsdb[{handler_type}]'")

        logger.debug("%s.create_tmp_handler: connection args - %s", self.__class__.__name__, connection_data)
        integration_id = int(time() * 10000)
        fs_store = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            resource_id=integration_id,
            root_dir='tmp',
            sync=False
        )
        handler_ars = self._make_handler_args(handler_type, connection_data)
        handler_ars['fs_store'] = fs_store
        handler_ars = dict(
            name='tmp_handler',
            fs_store=fs_store,
            connection_data=connection_data,
            integration_id=integration_id,
        )

        logger.debug("%s.create_tmp_handler: create a client to db of %s type", self.__class__.__name__, handler_type)
        return DBClient(handler_type, self.handler_modules[handler_type].Handler, **handler_ars)

    @profiler.profile()
    def get_handler(self, name, case_sensitive=False):
        handler = self.handlers_cache.get(name)
        if handler is not None:
            return handler

        if case_sensitive:
            integration_record = db.session.query(db.Integration).filter_by(company_id=ctx.company_id, name=name).first()
        else:
            integration_record = db.session.query(db.Integration).filter(
                (db.Integration.company_id == ctx.company_id)
                & (func.lower(db.Integration.name) == func.lower(name))
            ).first()

        integration_data = self._get_integration_record_data(integration_record, True)
        if integration_data is None:
            raise Exception(f"Can't find integration_record for handler '{name}'")
        connection_data = integration_data.get('connection_data', {})
        integration_engine = integration_data['engine']
        integration_name = integration_data['name']
        logger.debug("%s.get_handler: connection_data=%s, engine=%s", self.__class__.__name__, connection_data, integration_engine)

        if integration_engine not in self.handler_modules:
            raise Exception(f"Can't find handler for '{integration_name}' ({integration_engine})")

        integration_meta = self.handlers_import_status[integration_engine]
        if integration_meta["import"]["success"] is False:
            msg = dedent(f'''\
                Handler '{integration_engine}' cannot be used. Reason is:
                    {integration_meta['import']['error_message']}
            ''')
            is_cloud = Config().get('cloud', False)
            if is_cloud is False:
                msg += dedent(f'''

                If error is related to missing dependencies, then try to run command in shell and restart mindsdb:
                    pip install mindsdb[{integration_engine}]
                ''')
            logger.debug(msg)
            raise Exception(msg)

        connection_args = integration_meta.get('connection_args')
        logger.debug("%s.get_handler: connection args - %s", self.__class__.__name__, connection_args)

        fs_store = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            resource_id=integration_record.id,
            sync=True,
        )

        if isinstance(connection_args, (dict, OrderedDict)):
            files_to_get = {
                arg_name: arg_value for arg_name, arg_value in connection_data.items()
                if arg_name in connection_args and connection_args.get(arg_name)['type'] == ARG_TYPE.PATH
            }
            if len(files_to_get) > 0:

                for file_name, file_path in files_to_get.items():
                    connection_data[file_name] = fs_store.get_path(file_path)

        handler_ars = self._make_handler_args(integration_engine, connection_data)
        handler_ars['name'] = name
        handler_ars['file_storage'] = fs_store
        handler_ars['integration_id'] = integration_data['id']

        handler_type = self.handler_modules[integration_engine].type
        if handler_type == 'ml':
            handler_ars['storage_factory'] = FileStorageFactory(
                resource_group=RESOURCE_GROUP.PREDICTOR,
                sync=True
            )

        HandlerClass = self.handler_modules[integration_engine].Handler

        if isinstance(HandlerClass, type) and issubclass(HandlerClass, BaseMLEngine):
            handler_ars['handler_class'] = HandlerClass
            handler_ars['execution_method'] = getattr(self.handler_modules[integration_engine], 'execution_method', None)
            handler_ars['integration_engine'] = integration_engine
            logger.info("%s.get_handler: create a ML client, params - %s", self.__class__.__name__, handler_ars)
            handler = BaseMLEngineExec(**handler_ars)
            # handler = MLClient(**handler_ars)
        else:

            logger.info("%s.get_handler: create a client to db service of %s type, args - %s", self.__class__.__name__, integration_engine, handler_ars)
            handler = DBClient(integration_engine, HandlerClass, **handler_ars)

        return handler

    def reload_handler_module(self, handler_name):
        importlib.reload(self.handler_modules[handler_name])
        try:
            handler_meta = self._get_handler_meta(self.handler_modules[handler_name])
        except Exception as e:
            handler_meta = {
                'import': {
                    'success': False,
                    'error_message': str(e),
                    'dependencies': []
                },
                'name': handler_name
            }

        self.handlers_import_status[handler_meta['name']] = handler_meta

    def _read_dependencies(self, path):
        dependencies = []
        requirements_txt = Path(path).joinpath('requirements.txt')
        if requirements_txt.is_file():
            with open(str(requirements_txt), 'rt') as f:
                dependencies = [x.strip(' \t\n') for x in f.readlines()]
                dependencies = [x for x in dependencies if len(x) > 0]
        return dependencies

    def _get_handler_meta(self, module):
        handler_dir = Path(module.__path__[0])
        handler_folder_name = handler_dir.name
        dependencies = self._read_dependencies(handler_dir)

        self.handler_modules[module.name] = module
        import_error = getattr(module, 'import_error', None)
        handler_meta = {
            'import': {
                'success': import_error is None,
                'folder': handler_folder_name,
                'dependencies': dependencies
            },
            'version': module.version
        }
        if import_error is not None:
            handler_meta['import']['error_message'] = str(import_error)

        module_attrs = [attr for attr in [
            'connection_args_example',
            'connection_args',
            'description',
            'name',
            'type',
            'title'
        ] if hasattr(module, attr)]
        for attr in module_attrs:
            handler_meta[attr] = getattr(module, attr)

        # region icon
        if hasattr(module, 'icon_path'):
            icon_path = handler_dir.joinpath(module.icon_path)
            handler_meta['icon'] = {
                'name': icon_path.name,
                'type': icon_path.name[icon_path.name.rfind('.') + 1:].lower()
            }
            if handler_meta['icon']['type'] == 'svg':
                with open(str(icon_path), 'rt') as f:
                    handler_meta['icon']['data'] = f.read()
            else:
                with open(str(icon_path), 'rb') as f:
                    handler_meta['icon']['data'] = base64.b64encode(f.read()).decode('utf-8')
        # endregion
        if hasattr(module, 'permanent'):
            handler_meta['permanent'] = module.permanent
        else:
            if handler_meta.get('name') in ('files', 'views', 'lightwood'):
                handler_meta['permanent'] = True
            else:
                handler_meta['permanent'] = False

        return handler_meta

    def _load_handler_modules(self):
        mindsdb_path = Path(importlib.util.find_spec('mindsdb').origin).parent
        handlers_path = mindsdb_path.joinpath('integrations/handlers')

        # edge case: running from tests directory, find_spec finds the base folder instead of actual package
        if not os.path.isdir(handlers_path):
            mindsdb_path = Path(importlib.util.find_spec('mindsdb').origin).parent.joinpath('mindsdb')
            handlers_path = mindsdb_path.joinpath('integrations/handlers')

        self.handler_modules = {}
        self.handlers_import_status = {}
        for handler_dir in handlers_path.iterdir():
            if handler_dir.is_dir() is False or handler_dir.name.startswith('__'):
                continue
            handler_folder_name = str(handler_dir.name)

            try:
                handler_module = importlib.import_module(f'mindsdb.integrations.handlers.{handler_folder_name}')
                handler_meta = self._get_handler_meta(handler_module)
            except Exception as e:
                handler_name = handler_folder_name
                if handler_name.endswith('_handler'):
                    handler_name = handler_name[:-8]
                dependencies = self._read_dependencies(handler_dir)
                handler_meta = {
                    'import': {
                        'success': False,
                        'error_message': str(e),
                        'folder': handler_folder_name,
                        'dependencies': dependencies
                    },
                    'name': handler_name
                }

            self.handlers_import_status[handler_meta['name']] = handler_meta

    def get_handlers_import_status(self):
        return self.handlers_import_status


integration_controller = IntegrationController()
