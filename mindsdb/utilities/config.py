import os
import json


class Config(object):
    _config = {}

    def __init__(self, config='mindsdb/default_config.json'):
        if isinstance(config, dict):
            self._update_recursive(self._config, config)
        elif isinstance(config, str):
            self.merge(config)
        else:
            raise TypeError('argument must be string or dict')

    def merge(self, file_path):
        if os.path.isfile(file_path):
            with open(file_path, 'r') as fp:
                config = json.load(fp)
            self._update_recursive(self._config, config)

    def _update_recursive(self, a, b):
        for key in [x for x in a.keys() if x in b]:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                self._update_recursive(a[key], b[key])
            else:
                a[key] = b[key]
        for key in b.keys():
            if key not in a:
                a[key] = b[key]

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key, default=None):
        return self._config.get(key, default)

    def get_all(self):
        return self._config
