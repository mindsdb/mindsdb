import os
import json
from copy import deepcopy

from mindsdb.utilities.fs import create_directory


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

        paths = {
            'root': os.environ['MINDSDB_STORAGE_DIR']
        }

        # content - temporary storage for entities
        paths['content'] = os.path.join(paths['root'], 'content')
        # storage - persist storage for entities
        paths['storage'] = os.path.join(paths['root'], 'storage')
        paths['static'] = os.path.join(paths['root'], 'static')
        paths['tmp'] = os.path.join(paths['root'], 'tmp')
        paths['log'] = os.path.join(paths['root'], 'log')
        paths['cache'] = os.path.join(paths['root'], 'cache')

        for path_name in paths:
            create_directory(paths[path_name])

        self._default_config = {
            'permanent_storage': {
                'location': 'local'
            },
            'storage_dir': os.environ['MINDSDB_STORAGE_DIR'],
            'paths': paths,
            "log": {
                "level": {
                    "console": "INFO",
                    "file": "DEBUG",
                    "db": "WARNING"
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
            },
            "cache": {
                "type": "local"
            }
        }

        self._config = _merge_configs(self._default_config, self._override_config)

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key, default=None):
        return self._config.get(key, default)

    def get_all(self):
        return self._config

    @property
    def paths(self):
        return self._config['paths']
