import os
import json
import hashlib
import datetime
from copy import deepcopy

from mindsdb.utilities.fs import create_directory
from mindsdb.interfaces.state.db import session, Coonfiguration


def _merge_key_recursive(target_dict, source_dict, key):
    if key not in target_dict:
        target_dict[key] = source_dict[key]
    elif not isinstance(target_dict[key], dict) or not isinstance(source_dict[key], dict):
        target_dict[key] = source_dict[key]
    else:
        for k in list(source_dict[key].keys()):
            _merge_key_recursive(target_dict[key], source_dict[key], k)

def _merge_configs(config, other_config):
    for key in list(other_config.keys()):
        _merge_key_recursive(config, other_config, key)
    return config


class Config():
    paths = {
        'root': '',
        'datasources': '',
        'predictors': '',
        'static': '',
        'tmp': '',
        'log': ''
    }

    def __init__(self):
        if os.environ['MINDSDB_CONFIG_PATH'] != 'absent':
            self._override_config = {}
        else:
            with open(os.environ['MINDSDB_CONFIG_PATH'], 'r') as fp:
                self._override_config = json.load(fp)

        self.company_id = os.envrion.get('MINDSDB_COMPANY_ID', None)

        # Now comes the stuff that gets stored in the db
        if self._db_config is None:
            self._db_config = {
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
            self._db_config['paths']['root'] = os.envrion['MINDSDB_STORAGE_DIR']
            self._db_config['paths']['datasources'] = os.path.join(storage_dir, 'datasources')
            self._db_config['paths']['predictors'] = os.path.join(storage_dir, 'predictors')
            self._db_config['paths']['static'] = os.path.join(storage_dir, 'static')
            self._db_config['paths']['tmp'] = os.path.join(storage_dir, 'tmp')
            self._db_config['paths']['log'] = os.path.join(storage_dir, 'log')
            for path in self._db_config['paths']:
                create_directory(path)
            self._save()

    def _read(self):
        if isinstance(self.config_path, str) and os.path.isfile(self.config_path):
            with open(self.config_path, 'r') as fp:
                self._config = json.load(fp)
                self._validate()
                # @TODO: Overrid with user config
                #self._merge_default_config()
        else:
            raise TypeError('`self.config_path` must be a string representing a local file path to a json config')

    def _save(self):
        with open(self.config_path, 'w') as fp:
            json.dump(self._config, fp, indent=4, sort_keys=True)

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key, default=None):
        return self._config.get(key, default)

    def get_all(self):
        return self._config

    def set(self, key_chain, value, delete=False):
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
