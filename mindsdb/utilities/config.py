import os
import json
from copy import deepcopy
from pathlib import Path

from mindsdb.utilities.fs import create_directory, get_or_create_data_dir


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


config = None
config_mtime = -1


class Config():
    def __init__(self):
        # initialize once
        global config, config_mtime
        self.config_path = os.environ.get('MINDSDB_CONFIG_PATH', 'absent')
        self.use_docker_env = os.environ.get('MINDSDB_DOCKER_ENV', False)
        if self.use_docker_env:
            self.use_docker_env = True

        if Path(self.config_path).is_file():
            current_config_mtime = os.path.getmtime(self.config_path)
            if config_mtime != current_config_mtime:
                config = self.init_config()
                config_mtime = current_config_mtime

        if config is None:
            config = self.init_config()

        self._config = config

    def init_config(self):
        if self.config_path == 'absent':
            self._override_config = {}
        else:
            with open(self.config_path, 'r') as fp:
                self._override_config = json.load(fp)

        # region define storage dir
        if 'storage_dir' in self._override_config:
            root_storage_dir = self._override_config['storage_dir']
            os.environ['MINDSDB_STORAGE_DIR'] = root_storage_dir
        elif os.environ.get('MINDSDB_STORAGE_DIR') is not None:
            root_storage_dir = os.environ['MINDSDB_STORAGE_DIR']
        else:
            root_storage_dir = get_or_create_data_dir()
            os.environ['MINDSDB_STORAGE_DIR'] = root_storage_dir
        # endregion

        if os.path.isdir(root_storage_dir) is False:
            os.makedirs(root_storage_dir)

        if 'storage_db' in self._override_config:
            os.environ['MINDSDB_DB_CON'] = self._override_config['storage_db']
        elif os.environ.get('MINDSDB_DB_CON', '') == '':
            os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(root_storage_dir,
                                                                       'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

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
            'auth': {
                'http_auth_enabled': False,
                'username': 'mindsdb',
                'password': ''
            },
            "log": {
                "level": {
                    "console": "INFO",
                    "file": "DEBUG",
                    "db": "WARNING"
                }
            },
            "gui": {
                "autoupdate": True
            },
            "debug": False,
            "integrations": {},
            "api": {
                "http": {
                    "host": "127.0.0.1" if not self.use_docker_env else "0.0.0.0",
                    "port": "47334"
                },
                "mysql": {
                    "host": "127.0.0.1" if not self.use_docker_env else "0.0.0.0",
                    "password": "",
                    "port": "47335",
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

        return _merge_configs(self._default_config, self._override_config)

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key, default=None):
        return self._config.get(key, default)

    def get_all(self):
        return self._config

    @property
    def paths(self):
        return self._config['paths']
