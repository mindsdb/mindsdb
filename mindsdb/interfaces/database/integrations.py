import copy
import base64
import shutil
import tempfile
import importlib
from time import time
from pathlib import Path
from copy import deepcopy
from collections import OrderedDict

from sqlalchemy import func

from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore, FileStorage, FileStorageFactory, RESOURCE_GROUP
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE, HANDLER_TYPE
from mindsdb.utilities import log
from mindsdb.integrations.handlers_client.db_client import DBServiceClient
from mindsdb.interfaces.model.functions import get_model_records
from mindsdb.utilities.context import context as ctx


class IntegrationController:
    @staticmethod
    def _is_not_empty_str(s):
        return isinstance(s, str) and len(s) > 0

    def __init__(self):
        self._load_handler_modules()

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
        if engine in ['redis', 'kafka']:
            self._add_integration_record(name, engine, connection_args)
            return

        log.logger.debug(
            "%s: add method calling name=%s, engine=%s, connection_args=%s, company_id=%s",
            self.__class__.__name__, name, engine, connection_args, ctx.company_id
        )
        handlers_meta = self.get_handlers_import_status()
        handler_meta = handlers_meta[engine]
        accept_connection_args = handler_meta.get('connection_args')
        log.logger.debug("%s: accept_connection_args - %s", self.__class__.__name__, accept_connection_args)

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

        # check permanent integration
        if name in self.handler_modules:
            handler = self.handler_modules[name]

            if getattr(handler, 'permanent', False) is True:
                raise Exception('Unable to drop: is permanent integration')

        integration_record = db.session.query(db.Integration).filter_by(company_id=ctx.company_id, name=name).first()

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

        return {
            'id': integration_record.id,
            'name': integration_record.name,
            'type': integration_type,
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
            handler_ars['handler_controller'] = IntegrationController()
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
        as_service = False
        if 'as_service' in connection_data:
            as_service = connection_data["as_service"]
            connection_data = copy.deepcopy(connection_data)
            del connection_data['as_service']
            log.logger.debug("%s create_tmp_handler: delete 'as_service' key from connection args - %s", self.__class__.__name__, connection_data)
        resource_id = int(time() * 10000)
        fs_store = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            resource_id=resource_id,
            root_dir='tmp',
            sync=False
        )
        handler_ars = self._make_handler_args(handler_type, connection_data)
        handler_ars['fs_store'] = fs_store
        handler_ars = dict(
            name='tmp_handler',
            fs_store=fs_store,
            connection_data=connection_data
        )

        if as_service:
            log.logger.debug("%s create_tmp_handler: create a client to db of %s type", self.__class__.__name__, handler_type)
            return DBServiceClient(handler_type, as_service=as_service, **handler_ars)
        return self.handler_modules[handler_type].Handler(**handler_ars)

    def get_handler(self, name, case_sensitive=False):
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
        log.logger.debug("%s get_handler: connection_data=%s, engine=%s", self.__class__.__name__, connection_data, integration_engine)

        if integration_engine not in self.handler_modules:
            raise Exception(f"Can't find handler for '{integration_name}' ({integration_engine})")

        integration_meta = self.handlers_import_status[integration_engine]
        connection_args = integration_meta.get('connection_args')
        as_service = False
        if 'as_service' in connection_data:
            as_service = connection_data['as_service']
            del connection_data['as_service']
        log.logger.debug("%s get_handler: connection args - %s", self.__class__.__name__, connection_args)

        fs_store = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            resource_id=integration_record.id,
            sync=True,
        )

        if isinstance(connection_args, (dict, OrderedDict)):
            files_to_get = [
                arg_name for arg_name in connection_data
                if arg_name in connection_args and connection_args.get(arg_name)['type'] == ARG_TYPE.PATH
            ]
            if len(files_to_get) > 0:

                for file_name in files_to_get:
                    connection_data[file_name] = fs_store.get_path(file_name)

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
        from mindsdb.integrations.libs.base import BaseMLEngine
        from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec

        HandlerClass = self.handler_modules[integration_engine].Handler

        if isinstance(HandlerClass, type) and issubclass(HandlerClass, BaseMLEngine):
            handler_ars['handler_class'] = HandlerClass
            handler_ars['execution_method'] = getattr(self.handler_modules[integration_engine], 'execution_method', None)
            handler = BaseMLEngineExec(**handler_ars)
        else:
            handler = HandlerClass(**handler_ars)

        if as_service:
            log.logger.debug("%s get_handler: create a client to db service of %s type", self.__class__.__name__, handler_type)
            return DBServiceClient(handler_type, as_service=as_service, **handler_ars)

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
        import_error = None
        if hasattr(module, 'import_error'):
            import_error = module.import_error
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
