import os
import shutil
import tempfile
from pathlib import Path
from copy import deepcopy

from sqlalchemy import func

from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Integration
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities.fs import create_directory


def _is_not_empty_str(s):
    return isinstance(s, str) and len(s) > 0


def add_db_integration(name, data, company_id):
    if 'database_name' not in data:
        data['database_name'] = name
    if 'publish' not in data:
        data['publish'] = True

    bundle_path = data.get('secure_connect_bundle')
    if data.get('type') in ('cassandra', 'scylla') and _is_not_empty_str(bundle_path):
        if os.path.isfile(bundle_path) is False:
            raise Exception(f'Can not get access to file: {bundle_path}')
        integrations_dir = Config()['paths']['integrations']

        p = Path(bundle_path)
        data['secure_connect_bundle'] = p.name

        integration_record = Integration(name=name, data=data, company_id=company_id)
        session.add(integration_record)
        session.commit()
        integration_id = integration_record.id

        folder_name = f'integration_files_{company_id}_{integration_id}'
        integration_dir = os.path.join(integrations_dir, folder_name)
        create_directory(integration_dir)
        shutil.copyfile(bundle_path, os.path.join(integration_dir, p.name))

        FsStore().put(
            folder_name,
            integration_dir,
            integrations_dir
        )
    elif data.get('type') in ('mysql', 'mariadb'):
        ssl = data.get('ssl')
        files = {}
        temp_dir = None
        if ssl is True:
            for key in ['ssl_ca', 'ssl_cert', 'ssl_key']:
                if key not in data:
                    continue
                if os.path.isfile(data[key]) is False:
                    if _is_not_empty_str(data[key]) is False:
                        raise Exception("'ssl_ca', 'ssl_cert' and 'ssl_key' must be paths or inline certs")
                    if temp_dir is None:
                        temp_dir = tempfile.mkdtemp(prefix='integration_files_')
                    cert_file_name = data.get(f'{key}_name', f'{key}.pem')
                    cert_file_path = os.path.join(temp_dir, cert_file_name)
                    with open(cert_file_path, 'wt') as f:
                        f.write(data[key])
                    data[key] = cert_file_path
                files[key] = data[key]
                p = Path(data[key])
                data[key] = p.name
        integration_record = Integration(name=name, data=data, company_id=company_id)
        session.add(integration_record)
        session.commit()
        integration_id = integration_record.id

        if len(files) > 0:
            integrations_dir = Config()['paths']['integrations']
            folder_name = f'integration_files_{company_id}_{integration_id}'
            integration_dir = os.path.join(integrations_dir, folder_name)
            create_directory(integration_dir)
            for file_path in files.values():
                p = Path(file_path)
                shutil.copyfile(file_path, os.path.join(integration_dir, p.name))
            FsStore().put(
                folder_name,
                integration_dir,
                integrations_dir
            )
    else:
        integration_record = Integration(name=name, data=data, company_id=company_id)
        session.add(integration_record)
        session.commit()


def modify_db_integration(name, data, company_id):
    integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
    old_data = deepcopy(integration_record.data)
    for k in old_data:
        if k not in data:
            data[k] = old_data[k]

    integration_record.data = data
    session.commit()


def remove_db_integration(name, company_id):
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


def _get_integration_record_data(integration_record, sensitive_info=True):
    if integration_record is None or integration_record.data is None:
        return None
    data = deepcopy(integration_record.data)
    if data.get('password', None) is None:
        data['password'] = ''
    data['date_last_update'] = deepcopy(integration_record.updated_at)

    bundle_path = data.get('secure_connect_bundle')
    mysql_ssl_ca = data.get('ssl_ca')
    mysql_ssl_cert = data.get('ssl_cert')
    mysql_ssl_key = data.get('ssl_key')
    if (
        data.get('type') in ('mysql', 'mariadb')
        and (
            _is_not_empty_str(mysql_ssl_ca)
            or _is_not_empty_str(mysql_ssl_cert)
            or _is_not_empty_str(mysql_ssl_key)
        )
        or data.get('type') in ('cassandra', 'scylla')
        and bundle_path is not None
    ):
        fs_store = FsStore()
        integrations_dir = Config()['paths']['integrations']
        folder_name = f'integration_files_{integration_record.company_id}_{integration_record.id}'
        integration_dir = os.path.join(integrations_dir, folder_name)
        fs_store.get(
            folder_name,
            integration_dir,
            integrations_dir
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

    data['id'] = integration_record.id

    return data


def get_db_integration(name, company_id, sensitive_info=True, case_sensitive=False):
    if case_sensitive:
        integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
    else:
        integration_record = session.query(Integration).filter(
            (Integration.company_id == company_id)
            & (func.lower(Integration.name) == func.lower(name))
        ).first()
    return _get_integration_record_data(integration_record, sensitive_info)


def get_db_integrations(company_id, sensitive_info=True):
    integration_records = session.query(Integration).filter_by(company_id=company_id).all()
    integration_dict = {}
    for record in integration_records:
        if record is None or record.data is None:
            continue
        integration_dict[record.name] = _get_integration_record_data(record, sensitive_info)
    return integration_dict
