import unittest

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.storage_handler import RedisStorageHandler, SqliteStorageHandler

class StorageHandlerTest(unittest.TestCase):
    def test_1_redis_storage(self):
        store = RedisStorageHandler({'test_1_redis_storage': 'value'}, config={'host': 'localhost', 'port': '6379'})
        store.set('test_key', 42)
        self.assertTrue(store.get('test_key') == 42)
        self.assertRaises(Exception, store.get('test_key2'))

    def test_2_sqlite_storage(self):
        config = Config()
        name = 'test_2_sqlite_storage'
        store = SqliteStorageHandler(context=name, config={
            'path': config['paths']['root'],
            'name': name
        })
        store.set('test_key', 42)
        self.assertTrue(store.get('test_key') == 42)
        self.assertRaises(Exception, store.get('test_key2'))