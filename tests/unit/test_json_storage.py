import tempfile
import os
import unittest
import json

temp_dir = tempfile.mkdtemp(dir='/tmp/', prefix='lightwood_handler_test_')
os.environ['MINDSDB_STORAGE_DIR'] = os.environ.get('MINDSDB_STORAGE_DIR', temp_dir)
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'
from mindsdb.interfaces.storage import db

from mindsdb.migrations import migrate  # noqa

from mindsdb.interfaces.storage.json import get_json_storage  # noqa


class Test(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        # config
        config = {}
        # TODO run on own database
        fdi, cfg_file = tempfile.mkstemp(prefix='mindsdb_conf_')

        with os.fdopen(fdi, 'w') as fd:
            json.dump(config, fd)

        os.environ['MINDSDB_CONFIG_PATH'] = cfg_file

        db.init()
        migrate.migrate_to_head()


    def test_1_insert(self):
        storage_1 = get_json_storage(1)
        storage_1['x'] = {'y': 1}
        assert storage_1['x']['y'] == 1
        assert storage_1['x']['y'] == storage_1.get('x')['y']

        another_storage_1 = get_json_storage(1)
        assert another_storage_1['x']['y'] == storage_1['x']['y']

        storage_2 = get_json_storage(2)
        assert storage_2['x'] is None

        another_storage_2 = get_json_storage(2)
        another_storage_2.set('x', {'y': 2})
        assert storage_2['x']['y'] == 2

    def test_2_company_independent(self):
        storage_1 = get_json_storage(1, company_id=1)
        storage_1['x'] = {'y': 1}
        assert storage_1['x']['y'] == 1

        storage_2 = get_json_storage(1, company_id=2)
        assert storage_2['x'] is None
        storage_2['x'] = {'y': 2}
        assert storage_1['x']['y'] != storage_2['x']['y']


if __name__ == '__main__':
    unittest.main()
