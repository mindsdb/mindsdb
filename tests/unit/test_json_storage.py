import tempfile
import os
import unittest

temp_dir = tempfile.mkdtemp(dir='/tmp/', prefix='lightwood_handler_test_')
os.environ['MINDSDB_STORAGE_DIR'] = os.environ.get('MINDSDB_STORAGE_DIR', temp_dir)
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

from mindsdb.migrations import migrate  # noqa
migrate.migrate_to_head()

from mindsdb.interfaces.storage.json import get_storage # noqa


class Test(unittest.TestCase):
    def test_1_insert(self):
        storage_1 = get_storage(1)
        storage_1['x'] = {'y': 1}
        assert storage_1['x']['y'] == 1
        assert storage_1['x']['y'] == storage_1.get('x')['y']

        another_storage_1 = get_storage(1)
        assert another_storage_1['x']['y'] == storage_1['x']['y']

        storage_2 = get_storage(2)
        assert storage_2['x'] is None

        another_storage_2 = get_storage(2)
        another_storage_2.set('x', {'y': 2})
        assert storage_2['x']['y'] == 2

    def test_2_company_independent(self):
        storage_1 = get_storage(1, company_id=1)
        storage_1['x'] = {'y': 1}
        assert storage_1['x']['y'] == 1

        storage_2 = get_storage(1, company_id=2)
        assert storage_2['x'] is None
        storage_2['x'] = {'y': 2}
        assert storage_1['x']['y'] != storage_2['x']['y']


if __name__ == '__main__':
    unittest.main()
