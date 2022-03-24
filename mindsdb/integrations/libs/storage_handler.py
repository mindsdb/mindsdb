import os
import pickle
from typing import Dict
from hashlib import md5

import redis


class StorageHandler:
    """ 
    Simple key-value store. Instances of this handler shall store any information required by other handlers.
    Context should store anything relevant to the storage handler, e.g. CompanyID, UserID, parent handler name, among others. 
    """  # noqa
    def __init__(self, context: Dict, config=None):
        self.config = config if config else os.getenv('MDB_STORAGE_HANDLER_CONFIG')
        self.context = pickle.dumps(context)  # store serialized

    def _get_context_key(self, key: str):
        serialized_key = pickle.dumps(key)
        return md5(serialized_key).hexdigest() + md5(self.context).hexdigest()

    def get(self, key):
        serialized_value = self._get(self._get_context_key(key))
        if serialized_value:
            return pickle.loads(serialized_value)
        else:
            raise Exception("Key has no value stored in it!")

    def set(self, key: str, value: object):
        serialized_value = pickle.dumps(value)
        self._set(self._get_context_key(key), serialized_value)

    def _get(self, serialized_key):
        raise NotImplementedError()

    def _set(self, serialized_key, serialized_value):
        raise NotImplementedError()


class SqliteStorageHandler(StorageHandler):
    """ StorageHandler that uses SQLite as backend. """  # noqa
    def __init__(self, context: Dict, config=None):
        super().__init__(context, config)
        path = os.path.join(self.config.get("path", "./"), self.config.get("name", 'mlflow_integration_registry.db'))
        self.connection = sqlite3.connect(path)
        self._setup_connection()

    def _setup_connection(self):
        """ Checks that a key-value table exists, otherwise creates it. """  # noqa
        cur = self.connection.cursor()
        if ('store',) not in list(cur.execute("SELECT name FROM sqlite_master WHERE type='table';")):
            cur.execute(
                """create table store (key text, value text)""")
            self.internal_registry.commit()

    def _get(self, serialized_key):
        return self.connection(f"""select value from store where key={serialized_key}""")

    def _set(self, serialized_key, serialized_value):
        cur = self.connection.cursor()
        cur.execute("insert into store values (?, ?)", (serialized_key, serialized_value))
        self.connection.commit()


class RedisStorageHandler(StorageHandler):
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


if __name__ == '__main__':
    # todo convert to unit tests
    cls = StorageHandler({'test_context_key': 'value'}, config={'host': 'localhost', 'port': '6379'})
    cls.set('test_key', 42)
    assert cls.get('test_key') == 42

    cls2 = StorageHandler({'test_context_key': 'value2'}, config={'host': 'localhost', 'port': '6379'})
    try:
        cls2.get('test_key')  # todo turn into assertRaises once in a unit test
    except Exception:
        print("Key has no value stored in it, aborting...")
