"""
How to use it:

    from mindsdb.utilities.cache import get_cache, dataframe_checksum, json_checksum

    # namespace of cache
    cache = get_cache('predict')

    key = dataframe_checksum(df) # or json_checksum, depends on object type
    df_predict = cache(key)

    if df_predict is None:
        # no cache, save it
        df_predict = predictor.predict(df)
        cache.set(key, df_predict)



Configuration:

- max_size size of cache in count of records, default is 50
- serializer, module for serialization, default is dill

It can be set via:
- get_cache function:
    cache = get_cache('predict', max_size=2)
- using specific cache class:
    cache = FileCache('predict', max_size=2)
- using mindsdb config file:
    "cache": {
        "type": "redis",
        "max_size": 2
    }

Cache engines:

Can be specified in mindsdb config json. Possible values:
- local - for FileCache, default
- redis - for RedisCache
By default is used local redis server. You can specify
    "cache": {
        "type": "redis",
        "connection": {
            "host": "127.0.0.1",
            "port": 6379
        }
    }

How to test:

    env PYTHONPATH=./ pytest tests/unit/test_cache.py

"""

import os
import time
from abc import ABC
from pathlib import Path
import hashlib
import typing as t

import pandas as pd
import walrus

from mindsdb.utilities.config import Config
from mindsdb.utilities.json_encoder import CustomJSONEncoder


def dataframe_checksum(df: pd.DataFrame):
    checksum = str_checksum(df.to_json())
    return checksum


def json_checksum(obj: t.Union[dict, list]):
    checksum = str_checksum(CustomJSONEncoder().encode(obj))
    return checksum


def str_checksum(obj: str):
    checksum = hashlib.sha256(obj.encode()).hexdigest()
    return checksum


class BaseCache(ABC):
    def __init__(self, max_size=None, serializer=None):
        self.config = Config()
        if max_size is None:
            max_size = self.config["cache"].get("max_size", 50)
        self.max_size = max_size
        if serializer is None:
            serializer_module = self.config["cache"].get('serializer')
            if serializer_module == 'pickle':
                import pickle as s_module
            else:
                import dill as s_module
            self.serializer = s_module

    # default functions

    def set_df(self, name, df):
        return self.set(name, df)

    def get_df(self, name):
        return self.get(name)

    def serialize(self, value):
        return self.serializer.dumps(value)

    def deserialize(self, value):
        return self.serializer.loads(value)


class FileCache(BaseCache):
    def __init__(self, category, path=None, **kwargs):
        super().__init__(**kwargs)

        if path is None:
            path = self.config['paths']['cache']

        # include category
        cache_path = Path(path) / category
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)

        self.path = cache_path

    def clear_old_cache(self):
        # buffer to delete, to not run delete on every adding
        buffer_size = 5

        if self.max_size is None:
            return

        cur_count = len(os.listdir(self.path))

        # remove oldest
        if cur_count > self.max_size + buffer_size:

            files = sorted(Path(self.path).iterdir(), key=os.path.getmtime)
            for file in files[:cur_count - self.max_size]:
                self.delete_file(file)

    def file_path(self, name):
        return self.path / name

    def set_df(self, name, df):
        path = self.file_path(name)
        df.to_pickle(path)
        self.clear_old_cache()

    def set(self, name, value):
        path = self.file_path(name)
        value = self.serialize(value)

        with open(path, 'wb') as fd:
            fd.write(value)
        self.clear_old_cache()

    def get_df(self, name):
        path = self.file_path(name)

        if not os.path.exists(path):
            return None
        return pd.read_pickle(path)

    def get(self, name):
        path = self.file_path(name)

        if not os.path.exists(path):
            return None
        with open(path, 'rb') as fd:
            value = fd.read()
        value = self.deserialize(value)
        return value

    def delete(self, name):
        path = self.file_path(name)
        self.delete_file(path)

    def delete_file(self, path):
        os.unlink(path)


class RedisCache(BaseCache):
    def __init__(self, category, connection_info=None, **kwargs):
        super().__init__(**kwargs)

        self.category = category

        if connection_info is None:
            # if no params will be used local redis
            connection_info = self.config["cache"].get("connection", {})
        self.client = walrus.Database(**connection_info)

    def clear_old_cache(self, key_added):

        if self.max_size is None:
            return

        # buffer to delete, to not run delete on every adding
        buffer_size = 5

        cur_count = self.client.hlen(self.category)

        # remove oldest
        if cur_count > self.max_size + buffer_size:
            # 5 is buffer to delete, to not run delete on every adding

            keys = self.client.hgetall(self.category)
            # to list
            keys = list(keys.items())
            # sort by timestamp
            keys.sort(key=lambda x: x[1])

            for key, _ in keys[:cur_count - self.max_size]:
                self.delete_key(key)

    def redis_key(self, name):
        return f'{self.category}_{name}'

    def set(self, name, value):
        key = self.redis_key(name)
        value = self.serialize(value)

        self.client.set(key, value)
        # using key with category name to store all keys with modify time
        self.client.hset(self.category, key, int(time.time() * 1000))

        self.clear_old_cache(key)

    def get(self, name):
        key = self.redis_key(name)
        value = self.client.get(key)
        if value is None:
            # no value in cache
            return None
        return self.deserialize(value)

    def delete(self, name):
        key = self.redis_key(name)

        self.delete_key(key)

    def delete_key(self, key):
        self.client.delete(key)
        self.client.hdel(self.category, key)


class NoCache:
    '''
        class for no cache mode
    '''
    def __init__(self, *args, **kwargs):
        pass

    def get(self, name):
        return None

    def set(self, name, value):
        pass


def get_cache(category, **kwargs):
    config = Config()
    if config.get('cache')['type'] == 'redis':
        return RedisCache(category, **kwargs)
    if config.get('cache')['type'] == 'none':
        return NoCache(category, **kwargs)
    else:
        return FileCache(category, **kwargs)
