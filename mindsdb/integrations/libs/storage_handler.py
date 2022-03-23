import os
import pickle
from typing import Dict
from hashlib import md5

import redis


class StorageHandler:
    """
    Simple key-value store using redis. Instances of this handler shall store any information required by other handlers.
    """  # noqa
    def __init__(self, context: Dict, config=None):
        self.config = config if config else os.getenv('MDB_STORAGE_HANDLER_CONFIG')
        assert self.config.get('host', False)
        assert self.config.get('port', False)
        self.context = pickle.dumps(context)  # store serialized
        self.connection = redis.Redis(host=self.config['host'], port=self.config['port'])

    def _get_context_key(self, key: str):
        serialized_key = pickle.dumps(key)
        return md5(serialized_key).hexdigest() + md5(self.context).hexdigest()

    def get(self, key):
        serialized_value = self.connection.get(self._get_context_key(key))
        if serialized_value:
            return pickle.loads(serialized_value)
        else:
            raise Exception("Key has no value stored in it!")

    def set(self, key: str, value: object):
        serialized_value = pickle.dumps(value)
        self.connection.set(self._get_context_key(key), serialized_value)


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
