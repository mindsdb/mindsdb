import os
import json
import hashlib
import datetime
from mindsdb.interfaces.state.schemas import Configuration,
from mindsdb.utilities.fs import create_dirs_recursive

default_config = {
    "log": {
        "level": {
            "console": "ERROR",
            "file": "WARNING"
        }

    },
    "debug": False,
    "integrations": {},
    "api": {
        "http": {
            "host": "127.0.0.1",
            "port": "47334"
        },
        "mysql": {
            "host": "127.0.0.1",
            "password": "",
            "port": "47335",
            "user": "mindsdb",
            "database": "mindsdb",
            "ssl": True
        },
        "mongodb": {
            "host": "127.0.0.1",
            "port": "47336",
            "database": "mindsdb"
        }
    }
    ,"company_id": 199925
}

def _get_or_create_dir_struct():
    for _dir in get_paths():
        try:
            assert os.path.exists(_dir)
            assert os.access(_dir, os.W_OK) is True
            return _dir
        except Exception:
            pass

    for _dir in get_paths():
        try:
            create_directory(_dir)
            assert os.access(_dir, os.W_OK) is True
            return _dir
        except Exception:
            pass

    raise Exception('MindsDB storage directory does not exist and could not be created')

def _null_to_empty(config):
    ''' changing user input to formalised view
    '''
    for integration in config.get('integrations', {}).values():
        password = integration.get('password')
        password = '' if password is None else str(password)
        integration['password'] = str(password)

    password = self._config['api']['mysql'].get('password')
    password = '' if password is None else str(password)
    config['api']['mysql']['password'] = str(password)
    return config

def _merge_key_recursive(target_dict, source_dict, key):
    if key not in target_dict:
        target_dict[key] = source_dict[key]
    elif isinstance(target_dict[key], dict) and isinstance(source_dict[key], dict):
        for k in source_dict[key]:
            _merge_key_recursive(target_dict[key], source_dict[key], k)

def _merge_configs(config, other_config):
    for key in default_config:
        _merge_key_recursive(config, other_config, key)
    return config

class Config(object):
    def __init__(self, config_path=None):
        if config_path is None:
            config = json.loads(Configuration.query.filter_by(company_id=config).first().data)

            with open(config_path, 'r') as fp:
                config = _merge_configs(config, json.load(fp))

            storage_dir = config.get('storage_dir', _get_or_create_dir_struct())
            if os.path.isabs(storage_dir) is False:
                storage_dir = os.path.normpath(
                    os.path.join(
                        os.path.dirname(config_path),
                        storage_dir
                    )
                )

            config['paths']['root'] = storage_dir
            config['paths']['datasources'] = os.path.join(storage_dir, 'datasources')
            config['paths']['predictors'] = os.path.join(storage_dir, 'predictors')
            config['paths']['static'] = os.path.join(storage_dir, 'static')
            config['paths']['tmp'] = os.path.join(storage_dir, 'tmp')
            config['paths']['log'] = os.path.join(storage_dir, 'log')
            config['paths']['obsolete'] = {
                'predictors': os.path.join(storage_dir, 'obsolete', 'predictors'),
                'datasources': os.path.join(storage_dir, 'obsolete', 'datasources')
            }

            create_dirs_recursive(config['paths'])

            config = _merge_configs(config, default_config)
            config = _null_to_empty(config)

        self._read()
        self._save()

    @property
    def paths(self):
        return self._config['paths']

    def _read(self):
        self._config = json.loads(Configuration.query.filter_by(company_id=config).first().data)

    def _save(self):
        config_record = Configuration(company_id=self._config['company_id'],data=self._config)
        session.save_or_update(config_record)
        session.commit())

    def __getitem__(self, key):
        self._read()
        return self._config[key]

    def get(self, key, default=None):
        self._read()
        return self._config.get(key, default)

    def get_all(self):
        self._read()
        return self._config

    def set(self, key_chain, value, delete=False):
        # @TOOD Maybe add a mutex here ? But that seems a bit overkill to be honest
        self._read()
        c = self._config
        for i, k in enumerate(key_chain):
            if k in c and i + 1 < len(key_chain):
                c = c[k]
            elif k not in c and i + 1 < len(key_chain):
                c[k] = {}
                c = c[k]
            else:
                if delete:
                    del c[k]
                else:
                    c[k] = value
        self._save()

    # Higher level interface
    def add_db_integration(self, name, dict):
        dict['date_last_update'] = str(datetime.datetime.now()).split('.')[0]
        if 'database_name' not in dict:
            dict['database_name'] = name
        if 'publish' not in dict:
            dict['publish'] = True

        self.set(['integrations', name], dict)

    def modify_db_integration(self, name, dict):
        old_dict = self._config['integrations'][name]
        for k in old_dict:
            if k not in dict:
                dict[k] = old_dict[k]

        self.add_db_integration(name, dict)

    def remove_db_integration(self, name):
        self.set(['integrations', name], None, True)
