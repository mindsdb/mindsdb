import os
import dill
import pickle
from typing import Dict
from hashlib import md5

import redis
import sqlite3


class KVStorageHandler:
    """ 
    Simple key-value store. Instances of this handler shall store any information required by other handlers.
    Context should store anything relevant to the storage handler, e.g. CompanyID, UserID, parent handler name, among others. 
    """  # noqa
    def __init__(self, context: Dict, config=None):
        self.config = config if config else os.getenv('MDB_STORAGE_HANDLER_CONFIG')
        self.serializer = pickle if config.get('serializer', '') == 'pickle' else dill
        self.context = self.serializer.dumps(context)  # store serialized

    def _get_context_key(self, key: str):
        serialized_key = self.serializer.dumps(key)
        return md5(serialized_key).hexdigest() + md5(self.context).hexdigest()

    def get(self, key, default_value=None):
        serialized_value = self._get(self._get_context_key(key))
        if serialized_value:
            return self.serializer.loads(serialized_value)
        elif default_value is not None:
            return default_value
        else:
            raise KeyError(f"Key not found: {key}")

    def set(self, key: str, value: object):
        serialized_value = self.serializer.dumps(value)
        self._set(self._get_context_key(key), serialized_value)

    def _get(self, serialized_key):
        raise NotImplementedError()

    def _set(self, serialized_key, serialized_value):
        raise NotImplementedError()


class SqliteStorageHandler(KVStorageHandler):
    """ StorageHandler that uses SQLite as backend. """  # noqa
    def __init__(self, context: Dict, config=None):
        super().__init__(context, config)
        name = self.config["name"] if self.config["name"][-3:] == '.db' else self.config["name"] + '.db'
        path = os.path.join(self.config.get("path", "./"), name)
        self.connection = sqlite3.connect(path)
        self._setup_connection()

    def _setup_connection(self):
        """ Checks that a key-value table exists, otherwise creates it. """  # noqa
        cur = self.connection.cursor()
        if ('store',) not in list(cur.execute("SELECT name FROM sqlite_master WHERE type='table';")):
            cur.execute("""create table store (key text PRIMARY KEY, value text)""")
            self.connection.commit()

    def _get(self, serialized_key):
        cur = self.connection.cursor()
        results = list(cur.execute(f"""select value from store where key='{serialized_key}'"""))
        if results:
            return results[0][0]  # should always be a single match, hence the [0]s
        else:
            return None

    def _set(self, serialized_key, serialized_value):
        cur = self.connection.cursor()
        cur.execute("insert or replace into store values (?, ?)", (serialized_key, serialized_value))
        self.connection.commit()


class RedisStorageHandler(KVStorageHandler):
    """ StorageHandler that uses Redis as backend. """  # noqa
    def __init__(self, context: Dict, config=None):
        super().__init__(context, config)
        assert self.config.get('host', False)
        assert self.config.get('port', False)

        self.connection = redis.Redis(host=self.config['host'], port=self.config['port'])

    def _get(self, serialized_key):
        return self.connection.get(serialized_key)

    def _set(self, serialized_key, serialized_value):
        self.connection.set(serialized_key, serialized_value)
