import os
import json
import hashlib
import datetime


class Config(object):
    current_version = '1.2'
    _config = {}
    paths = {
        'root': '',
        'datasources': '',
        'predictors': '',
        'static': '',
        'tmp': ''
    }
    versions = {}

    def __init__(self, config_path):
        self._config_path = None
        self._config_hash = None
        self._config = None
        if isinstance(config_path, str):
            self.config_path = config_path
            self._read()
            self._config_hash = self._gen_hash()

            storage_dir = self._config['storage_dir']
            self.paths['root'] = storage_dir
            self.paths['datasources'] = os.path.join(storage_dir, 'datasources')
            self.paths['predictors'] = os.path.join(storage_dir, 'predictors')
            self.paths['static'] = os.path.join(storage_dir, 'static')
            self.paths['tmp'] = os.path.join(storage_dir, 'tmp')

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
            shutil.move(
                str(ds_storage_path),
                temp_dir_path
            )
            ds_storage_path.mkdir(mode=0o777, exist_ok=True, parents=True)
            shutil.move(
                str(Path(temp_dir_path).joinpath('datastore')),
                str(ds_storage_path.joinpath('datasources'))
            )
            shutil.move(
                str(mdb_storage_path),
                str(ds_storage_path.joinpath('predictors'))
            )
            ds_storage_path.joinpath('tmp').mkdir(mode=0o777, exist_ok=True)
            ds_storage_path.joinpath('static').mkdir(mode=0o777, exist_ok=True)

            if Path(temp_dir_path).is_dir():
                shutil.rmtree(temp_dir_path)

            config['storage_dir'] = str(ds_storage_path)
            del config['interface']['datastore']['storage_dir']
            del config['interface']['mindsdb_native']['storage_dir']
            config['config_version'] = '1.2'
            return config

        migrations = {
            '1.0': m1_0,
            '1.1': m1_1
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

    def _read(self):
        if isinstance(self.config_path, str) and os.path.isfile(self.config_path):
            with open(self.config_path, 'r') as fp:
                self._config = json.load(fp)
                if self._parse_version(self._config['config_version']) < self._parse_version(self.current_version):
                    self._migrate()
                    self._save()
                self._validate()
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
        with open(self.config_path, 'r') as fp:
            self._config = json.load(fp)

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
        if 'enabled' not in dict:
            dict['enabled'] = True

        self.set(['integrations', name], dict)

    def modify_db_integration(self, name, dict):
        old_dict = self._config['integrations'][name]
        for k in old_dict:
            if k not in dict:
                dict[k] = old_dict[k]

        self.add_db_integration(name, dict)

    def remove_db_integration(self, name):
        self.set(['integrations', name], None, True)
