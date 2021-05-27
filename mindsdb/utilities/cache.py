import shelve
import os

from mindsdb.utilities.config import Config


class Cache:
    def __init__(self, name, *args, **kwargs):
        self.kwargs = kwargs
        self.cache_file = os.path.join(Config()['paths']['cache'], name)
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
        res = self.cache.__exit__(_type, value, traceback)
        self.cache = None
        return res

    def __contains__(self, key):
        return key in self.cache

    def __del__(self):
        try:
            self.cache.close()
        except Exception:
            pass
        os.remove(self.cache_file)
