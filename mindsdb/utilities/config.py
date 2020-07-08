import os
import json
import hashlib

class Config(object):
    _config = {}

    def __init__(self, config_path):
        self._config_path = None
        self._config_hash = None
        self._config = None
        if isinstance(config_path, str):
            self.config_path = config_path
            self._read()
            self._config_hash = self._gen_hash()
        else:
            raise TypeError('Argument must be string representing a file path <Later on to be switched to file path and/or database connection info>')

    def _read(self):
        if isinstance(self.config_path, str) and os.path.isfile(self.config_path):
            with open(self.config_path, 'r') as fp:
                self._config = config = json.load(fp)
        else:
            raise TypeError('`self.config_path` must be a string representing a local file path to a json config')

    def _gen_hash(self):
        with open(self.config_path, 'rb') as fp:
            return hashlib.md5(fp.read()).hexdigest()

    def _set_updated(self, key):
        # Only check this for dynamically generated keys, won't be needed once we switch to using a database here
        if key in self._config['integrations']:
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

    def set(self, key_chain, value):
        pass

    # Higher level interface
    def add_db_integration(self, name, dict):
        pass

    def modify_db_integration(self, name, dict):
        pass
