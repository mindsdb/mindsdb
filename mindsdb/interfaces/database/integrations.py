import os
import base64
import shutil
import tempfile
import importlib
from pathlib import Path
from copy import deepcopy
from collections import OrderedDict

from sqlalchemy import func

from mindsdb.interfaces.storage.db import session, Integration
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.interfaces.database.views import ViewController
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE, HANDLER_TYPE


class IntegrationController:
    @staticmethod
    def _is_not_empty_str(s):
        return isinstance(s, str) and len(s) > 0

    def __init__(self):
        self._load_handler_modules()

    def _add_integration_record(self, name, engine, connection_args, company_id=None):
        integration_record = Integration(
            name=name,
            engine=engine,
            data=connection_args,
            company_id=company_id
        )
        session.add(integration_record)
        session.commit()
        return integration_record.id

    def add(self, name, engine, connection_args, company_id=None):
        if engine in ['redis', 'kafka']:
            self._add_integration_record(name, engine, connection_args, company_id)
            return
        
        handlers_meta = self.get_handlers_import_status()
        handler_meta = handlers_meta[engine]
        accept_connection_args = handler_meta.get('connection_args')

        temp_dir = None
        if accept_connection_args is not None:
            for arg_name, arg_value in connection_args.items():
                if (
                    arg_name in accept_connection_args
                    and accept_connection_args[arg_name]['type'] == ARG_TYPE.PATH
                ):
                    if temp_dir is None:
                        temp_dir = tempfile.mkdtemp(prefix='mindsdb_files_')
                    shutil.copy(arg_value, temp_dir)
                    connection_args[arg_name] = Path(arg_value).name

        integration_id = self._add_integration_record(name, engine, connection_args, company_id)

        if temp_dir is not None:
            folder_name = f'integration_files_{company_id}_{integration_id}'
            integrations_dir = Config()['paths']['integrations']
            integration_data_dir = os.path.join(integrations_dir, folder_name)
            shutil.move(temp_dir, integration_data_dir)
            FsStore().put(
                folder_name,
                base_dir=integrations_dir
            )

    def modify(self, name, data, company_id):
        integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
        old_data = deepcopy(integration_record.data)
        for k in old_data:
            if k not in data:
                data[k] = old_data[k]

        integration_record.data = data
        session.commit()

    def delete(self, name, company_id=None):
        integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
        integrations_dir = Config()['paths']['integrations']
        folder_name = f'integration_files_{company_id}_{integration_record.id}'
        integration_dir = os.path.join(integrations_dir, folder_name)
        if os.path.isdir(integration_dir):
            shutil.rmtree(integration_dir)
        try:
            FsStore().delete(folder_name)
        except Exception:
            pass
        session.delete(integration_record)
        session.commit()

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

        return {
            'id': integration_record.id,
            'name': integration_record.name,
            'engine': integration_record.engine,
            'date_last_update': deepcopy(integration_record.updated_at),
            'connection_data': data
        }

    def get_by_id(self, integration_id, company_id=None, sensitive_info=True):
        integration_record = session.query(Integration).filter_by(company_id=company_id, id=integration_id).first()
        return self._get_integration_record_data(integration_record, sensitive_info)

    def get(self, name, company_id=None, sensitive_info=True, case_sensitive=False):
        if case_sensitive:
            integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
        else:
            integration_record = session.query(Integration).filter(
                (Integration.company_id == company_id)
                & (func.lower(Integration.name) == func.lower(name))
            ).first()
        return self._get_integration_record_data(integration_record, sensitive_info)

    def get_all(self, company_id=None, sensitive_info=True):
        integration_records = session.query(Integration).filter_by(company_id=company_id).all()
        integration_dict = {}
        for record in integration_records:
            if record is None or record.data is None:
                continue
            integration_dict[record.name] = self._get_integration_record_data(record, sensitive_info)
        return integration_dict

    def check_connections(self):
        connections = {}
        for integration_name, integration_meta in self.get_all().items():
            handler = self.create_handler(
                handler_type=integration_meta['engine'],
                connection_data=integration_meta['connection_data']
            )
            status = handler.check_connection()
            connections[integration_name] = status.get('success', False)
        return connections

    def create_handler(self, name: str = None, handler_type: str = None,
                       connection_data: dict = {}, company_id: int = None):
        fs_store = FsStore()

        handler_ars = dict(
            name=name,
            db_store=None,
            fs_store=fs_store,
            connection_data=connection_data
        )

        if handler_type == 'files':
            handler_ars['file_controller'] = WithKWArgsWrapper(
                FileController(),
                company_id=company_id
            )
        elif handler_type == 'views':
            handler_ars['view_controller'] = WithKWArgsWrapper(
                ViewController(),
                company_id=company_id
            )
        elif self.handler_modules.get(handler_type, False).type == HANDLER_TYPE.ML:
            handler_ars['handler_controller'] = WithKWArgsWrapper(
                IntegrationController(),
                company_id=company_id
            )
            handler_ars['company_id'] = company_id

        return self.handler_modules[handler_type].Handler(**handler_ars)

    def get_handler(self, name, company_id=None, case_sensitive=False):
        if case_sensitive:
            integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
        else:
            integration_record = session.query(Integration).filter(
                (Integration.company_id == company_id)
                & (func.lower(Integration.name) == func.lower(name))
            ).first()

        # test override for ludwig, REMOVE LATER
        # if name == 'lightwood':
        #     name = 'ludwig'

        # TODO del in future
        if integration_record is None:
            if name in self.handler_modules.keys():
                handler = self.create_handler(
                    name=name,
                    handler_type=name,
                    connection_data={},
                    company_id=company_id
                )
                return handler
            else:
                raise Exception(f'Unknown integration: {name}')

        # if integration_record.engine == 'lightwood':
        #     integration_record.engine = 'ludwig'
        #     integration_record.name = 'ludwig'
        integration_data = self._get_integration_record_data(integration_record, True)
        connection_data = integration_data.get('connection_data', {})
        integration_engine = integration_data['engine']
        integration_name = integration_data['name']

        if integration_engine not in self.handler_modules:
            raise Exception(f"Cant find handler for '{integration_name}' ({integration_engine})")

        integration_meta = self.handlers_import_status[integration_engine]
        connection_args = integration_meta.get('connection_args')
        if isinstance(connection_args, (dict, OrderedDict)):
            files_to_get = [
                arg_name for arg_name in connection_data
                if connection_args.get(arg_name)['type'] == ARG_TYPE.PATH
            ]
            if len(files_to_get) > 0:
                try:
                    folder_name = f'integration_files_{company_id}_{integration_record.id}'
                    integrations_dir = Config()['paths']['integrations']
                    FsStore().get(folder_name, base_dir=integrations_dir)
                except Exception:
                    pass
                for file_name in files_to_get:
                    connection_data[file_name] = (
                        Path(integrations_dir)
                        .joinpath(folder_name)
                        .joinpath(connection_data[file_name])
                    )

        handler = self.create_handler(
            name=integration_name,
            handler_type=integration_engine,
            connection_data=connection_data,
            company_id=company_id
        )

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
        handler_name = handler_folder_name
        if handler_name.endswith('_handler'):
            handler_name = handler_name[:-8]

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
