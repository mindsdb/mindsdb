import os
import json
import hashlib
import datetime
from copy import deepcopy

from mindsdb.utilities.fs import create_directory
from mindsdb.interfaces.storage.db import session, Configuration


def _null_to_empty(config):
    '''
    changing user input to formalised view
    '''
    for integration in config.get('integrations', {}).values():
        password = integration.get('password')
        password = '' if password is None else str(password)
        integration['password'] = str(password)

    password = config['api']['mysql'].get('password')
    password = '' if password is None else str(password)
    config['api']['mysql']['password'] = str(password)
    return config

def _merge_key_recursive(target_dict, source_dict, key):
    if key not in target_dict:
        target_dict[key] = source_dict[key]
    elif not isinstance(target_dict[key], dict) or not isinstance(source_dict[key], dict):
        target_dict[key] = source_dict[key]
    else:
        for k in list(source_dict[key].keys()):
            _merge_key_recursive(target_dict[key], source_dict[key], k)

def _merge_configs(original_config, override_config):
    original_config = deepcopy(original_config)
    for key in list(override_config.keys()):
        _merge_key_recursive(original_config, override_config, key)
    return original_config


class Config():
    def __init__(self):
        self.config_path = os.environ['MINDSDB_CONFIG_PATH']
        if self.config_path == 'absent':
            self._override_config = {}
        else:
            with open(self.config_path, 'r') as fp:
                self._override_config = json.load(fp)

        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self._db_config = {}
        self.last_updated = datetime.datetime.now() - datetime.timedelta(days=3600)
        self._read()
        self.last_updated = datetime.datetime.now() - datetime.timedelta(days=3600)

        # Now comes the stuff that gets stored in the db
        if len(self._db_config) == 0:
            self._db_config = {
                'permanent_storage': {
                    'location': 'local'
                },
                'paths': {},
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
            }

            self._db_config['paths']['root'] = os.environ['MINDSDB_STORAGE_DIR']
            self._db_config['paths']['datasources'] = os.path.join(self._db_config['paths']['root'], 'datasources')
            self._db_config['paths']['predictors'] = os.path.join(self._db_config['paths']['root'], 'predictors')
            self._db_config['paths']['custom_models'] = os.path.join(self._db_config['paths']['root'], 'custom_models')
            self._db_config['paths']['static'] = os.path.join(self._db_config['paths']['root'], 'static')
            self._db_config['paths']['tmp'] = os.path.join(self._db_config['paths']['root'], 'tmp')
            self._db_config['paths']['log'] = os.path.join(self._db_config['paths']['root'], 'log')
            self._db_config['paths']['storage_dir'] = self._db_config['paths']['root']
            self._db_config['storage_dir'] = self._db_config['paths']['root']

            for path_name in self._db_config['paths']:
                create_directory(self._db_config['paths'][path_name])
            self._save()
        self._read()

    def _read(self):
        # No need for instant sync unless we're on the same API
        # Hacky, but doesn't break any constraints that we were imposing before
        # There's no guarantee of syncing for the calls from the different APIs anyway, doing this doesn't change that
        # `True` to disable this until we add some sleepy time to our tests
        #if True or (datetime.datetime.now() - self.last_updated).total_seconds() > 2:
        config_record =  session.query(Configuration).filter(Configuration.company_id == self.company_id).filter(Configuration.updated_at >= self.last_updated).first()

        if config_record is not None:
            self._db_config = json.loads(config_record.data)

        self._config = _merge_configs(self._db_config, self._override_config)
        self.last_updated = datetime.datetime.now()


    def _save(self):
        self._db_config = _null_to_empty(self._db_config)
        config_record = session.query(Configuration).filter_by(company_id=self.company_id).first()

        if config_record is not None:
            config_record.data = json.dumps(self._db_config)
        else:
            config_record = Configuration(company_id=self.company_id, data=json.dumps(self._db_config))
            session.add(config_record)

        session.commit()

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
        self._read()
        c = self._db_config
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
        self._read()

    @property
    def paths(self):
        self._read()
        return self._config['paths']

    # Higher level interface
    def add_db_integration(self, name, dict):
        dict['date_last_update'] = str(datetime.datetime.now()).split('.')[0]
        if 'database_name' not in dict:
            dict['database_name'] = name
        if 'publish' not in dict:
            dict['publish'] = True

        self.set(['integrations', name], dict)

    def modify_db_integration(self, name, dict):
        self._read()
        old_dict = self._config['integrations'][name]
        for k in old_dict:
            if k not in dict:
                dict[k] = old_dict[k]

        self.add_db_integration(name, dict)

    def remove_db_integration(self, name):
        self.set(['integrations', name], None, True)
