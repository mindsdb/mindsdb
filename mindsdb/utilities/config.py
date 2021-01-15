import os
import json
import hashlib
import datetime


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
}


class Config(object):
    current_version = '1.4'
    _config = {}
    paths = {
        'root': '',
        'datasources': '',
        'predictors': '',
        'static': '',
        'tmp': '',
        'log': '',
        'obsolete': {
            'predictors': '',
            'datasources': ''
        }
    }
    versions = {}

    def __init__(self, config_path):
        self._config_path = None
        self._config_hash = None
        self._config = None
        if isinstance(config_path, str):
            self.config_path = os.path.abspath(config_path)
            self._read()
            self._config_hash = self._gen_hash()

            storage_dir = self._config['storage_dir']
            if os.path.isabs(storage_dir) is False:
                storage_dir = os.path.normpath(
                    os.path.join(
                        os.path.dirname(self.config_path),
                        storage_dir
                    )
                )
            self.paths['root'] = storage_dir
            self.paths['datasources'] = os.path.join(storage_dir, 'datasources')
            self.paths['predictors'] = os.path.join(storage_dir, 'predictors')
            self.paths['static'] = os.path.join(storage_dir, 'static')
            self.paths['tmp'] = os.path.join(storage_dir, 'tmp')
            self.paths['log'] = os.path.join(storage_dir, 'log')
            self.paths['obsolete']['predictors'] = os.path.join(storage_dir, 'obsolete', 'predictors')
            self.paths['obsolete']['datasources'] = os.path.join(storage_dir, 'obsolete', 'datasources')

            self._read_versions_file(os.path.join(self.paths['root'], 'versions.json'))
        else:
            raise TypeError('Argument must be string representing a file path <Later on to be switched to file path and/or database connection info>')

    def _read_versions_file(self, path):
        if os.path.isfile(path):
            with open(path, 'rt') as f:
                self.versions = json.loads(f.read())

    def _migrate(self):
        def m1_0(config):
            if 'default_clickhouse' in config['integrations'] and 'type' not in config['integrations']['default_clickhouse']:
                config['integrations']['default_clickhouse']['type'] = 'clickhouse'
            if 'default_mariadb' in config['integrations'] and 'type' not in config['integrations']['default_mariadb']:
                config['integrations']['default_mariadb']['type'] = 'mariadb'
            if 'datasources' in config['api']['mysql']:
                del config['api']['mysql']['datasources']
            config['config_version'] = '1.1'
            return config

        def m1_1(config):
            import tempfile
            import shutil
            from pathlib import Path

            ds_storage_path = Path(config['interface']['datastore']['storage_dir'])
            mdb_storage_path = Path(config['interface']['mindsdb_native']['storage_dir'])

            temp_dir_path = tempfile.mkdtemp()

            if ds_storage_path.is_dir():
                shutil.move(
                    str(ds_storage_path),
                    temp_dir_path
                )

            ds_storage_path.mkdir(mode=0o777, exist_ok=True, parents=True)

            if Path(temp_dir_path).joinpath('datastore').is_dir():
                shutil.move(
                    str(Path(temp_dir_path).joinpath('datastore')),
                    str(ds_storage_path.joinpath('datasources'))
                )
            else:
                ds_storage_path.joinpath('datasources').mkdir(mode=0o777, exist_ok=True)

            if ds_storage_path == mdb_storage_path:
                shutil.move(
                    str(Path(temp_dir_path)),
                    str(ds_storage_path.joinpath('predictors'))
                )
            elif mdb_storage_path.is_dir():
                shutil.move(
                    str(mdb_storage_path),
                    str(ds_storage_path.joinpath('predictors'))
                )
            else:
                mdb_storage_path.joinpath('predictors').mkdir(mode=0o777, exist_ok=True)

            ds_storage_path.joinpath('tmp').mkdir(mode=0o777, exist_ok=True)
            ds_storage_path.joinpath('static').mkdir(mode=0o777, exist_ok=True)

            if Path(temp_dir_path).is_dir():
                shutil.rmtree(temp_dir_path)

            config['storage_dir'] = str(ds_storage_path)
            del config['interface']['datastore']['storage_dir']
            del config['interface']['mindsdb_native']['storage_dir']
            config['config_version'] = '1.2'
            return config

        def m1_2(config):
            ''' remove no longer needed fields
            '''
            try:
                del config['api']['mysql']['log']
            except Exception:
                pass

            try:
                del config['interface']
            except Exception:
                pass

            if 'pip_path' in config and config['pip_path'] is None:
                del config['pip_path']

            if 'python_interpreter' in config and config['python_interpreter'] is None:
                del config['python_interpreter']

            config['config_version'] = '1.3'
            return config

        def m1_3(config):
            ''' rename integration['enabled'] to integration['publish']
            '''
            for integration in config.get('integrations', []).values():
                if 'enabled' in integration:
                    enabled = integration['enabled']
                    del integration['enabled']
                    integration['publish'] = enabled

            config['config_version'] = '1.4'
            return config

        migrations = {
            '1.0': m1_0,
            '1.1': m1_1,
            '1.2': m1_2,
            '1.3': m1_3
        }

        current_version = self._parse_version(self._config['config_version'])
        target_version = self._parse_version(self.current_version)
        while current_version < target_version:
            str_version = '.'.join([str(x) for x in current_version])
            self._config = migrations[str_version](self._config)
            current_version = self._parse_version(self._config['config_version'])

    def _validate(self):
        integrations = self._config.get('integrations', {})
        for key, value in integrations.items():
            if not isinstance(value, dict):
                raise TypeError(f"Config error: integration '{key}' must be a json")
            if 'type' not in integrations[key]:
                raise KeyError(f"Config error: for integration '{key}' key 'type' must be specified")

        storage_dir = self._config.get('storage_dir')
        if storage_dir is None:
            raise KeyError("'storage_dir' mandatory key in config")

    def _parse_version(self, version):
        if isinstance(version, str):
            version = [int(x) for x in version.split('.')]
        elif isinstance(version, int):
            version = [version]
        if len(version) == 1:
            version.append(0)
        return version

    def _format(self):
        ''' changing user input to formalised view
        '''
        for integration in self._config.get('integrations', {}).values():
            password = integration.get('password')
            password = '' if password is None else str(password)
            integration['password'] = str(password)

        password = self._config['api']['mysql'].get('password')
        password = '' if password is None else str(password)
        self._config['api']['mysql']['password'] = str(password)

    def _merge_default_config(self):
        def merge_key_recursive(target_dict, source_dict, key):
            if key not in target_dict:
                target_dict[key] = source_dict[key]
            elif isinstance(target_dict[key], dict) and isinstance(source_dict[key], dict):
                for k in source_dict[key]:
                    merge_key_recursive(target_dict[key], source_dict[key], k)

        for key in default_config:
            merge_key_recursive(self._config, default_config, key)

    def _read(self):
        if isinstance(self.config_path, str) and os.path.isfile(self.config_path):
            with open(self.config_path, 'r') as fp:
                self._config = json.load(fp)
                if self._parse_version(self._config['config_version']) < self._parse_version(self.current_version):
                    self._migrate()
                    self._save()
                self._validate()
                self._merge_default_config()
                self._format()
        else:
            raise TypeError('`self.config_path` must be a string representing a local file path to a json config')

    def _save(self):
        with open(self.config_path, 'w') as fp:
            json.dump(self._config, fp, indent=4, sort_keys=True)

    def _gen_hash(self):
        with open(self.config_path, 'rb') as fp:
            return hashlib.md5(fp.read()).hexdigest()

    def _set_updated(self, key):
        # Only check this for dynamically generated keys, won't be needed once we switch to using a database here
        if key in ['integrations']:
            file_hash = self._gen_hash()
            if file_hash != self._config_hash:
                self._read()
                self._config_hash = self._gen_hash()

    def __getitem__(self, key):
        self._set_updated(key)
        return self._config[key]

    def get(self, key, default=None):
        self._set_updated(key)
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
