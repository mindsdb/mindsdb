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
        storage = get_storage(1)
        storage['x'] = {'y': 1}
        assert storage['x']['y'] == 1

        another_storage = get_storage(1)
        assert another_storage['x']['y'] == storage['x']['y']


if __name__ == '__main__':
    unittest.main()
