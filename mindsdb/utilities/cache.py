import os
import shelve
import json
from abc import ABC, abstractmethod

import walrus
from mindsdb.utilities.config import Config

CONFIG = Config()


class BaseCache(ABC):
    def __init__(self):
        self.config = Config()

    @abstractmethod
    def delete(self):
        pass

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass



class LocalCache(BaseCache):
    def __init__(self, name, *args, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        self.cache_file = os.path.join(self.config['paths']['cache'], name)
        self.cache = shelve.open(self.cache_file, **kwargs)

    def __getattr__(self, name):
        return getattr(self.cache, name)

    def __getitem__(self, key):
        return self.cache.__getitem__(key)

    def __setitem__(self, key, value):
        return self.cache.__setitem__(key, value)

    def __enter__(self):
        if self.cache is None:
            self.cache = shelve.open(self.cache_file, **self.kwargs)
        return self.cache.__enter__()

    def __exit__(self, _type, value, traceback):
        if self.cache is None:
            return None
        res = self.cache.__exit__(_type, value, traceback)
        self.cache = None
        return res

    def __contains__(self, key):
        return key in self.cache

    def delete(self):
        try:
            self.cache.close()
        except Exception:
            pass
        os.remove(self.cache_file)


class RedisCache(BaseCache):
    def __init__(self, prefix, *args, **kwargs):
        super().__init__()
        self.prefix = prefix
        if self.config["cache"]["type"] != "redis":
            raise Exception(f"wrong cache type in config. expected 'redis', but got {self.config['cache']['type']}.")
        connection_info = self.config["cache"]["params"]
        self.client = walrus.Database(**connection_info)

    def __decode(self, data):

        if isinstance(data, dict):
            return dict((self.__decode(x), self.__decode(data[x])) for x in data)
        if isinstance(data, list):
            return list(self.__decode(x) for x in data)
        # assume it is string
        return data.decode("utf8")

    def __contains__(self, key):
        key = f"{self.prefix}_{key}"
        return key in self.__decode(self.client.keys())

    def __getitem__(self, key):
        key = f"{self.prefix}_{key}"
        raw = self.client.get(key)
        if raw is None:
            raise KeyError(key)
        try:
            res = json.loads(raw)
        except json.JSONDecodeError:
            res = raw.decode('utf8')
        return res

    def __setitem__(self, key, value):
        key = f"{self.prefix}_{key}"
        self.client.set(key, json.dumps(value))

    def __iter__(self):
        return iter(self.__decode(self.client.keys()))

    def __next__(self):
        for i in self.__decode(self.client.keys()):
            yield i

    def __delitem__(self, key):
        key = f"{self.prefix}_key"
        self.client.delete(key)

    def delete(self):
        pass


Cache = RedisCache if CONFIG['cache']['type'] == 'redis' else LocalCache
