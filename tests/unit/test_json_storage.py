import tempfile
import os
import unittest

temp_dir = tempfile.mkdtemp(dir='/tmp/', prefix='lightwood_handler_test_')
os.environ['MINDSDB_STORAGE_DIR'] = os.environ.get('MINDSDB_STORAGE_DIR', temp_dir)
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

from mindsdb.migrations import migrate  # noqa
migrate.migrate_to_head()

from mindsdb.interfaces.storage.json import JsonStorage # noqa


def get_storage(resource_id, resource_group='predictor', company_id=None):
    return JsonStorage(
        resource_group=resource_group,
        resource_id=resource_id,
        company_id=company_id
    )


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


if __name__ == '__main__':
    unittest.main()
