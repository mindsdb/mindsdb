import tempfile
import os
import unittest
import json

from mindsdb.interfaces.storage import db   # noqa
from mindsdb.migrations import migrate  # noqa
from mindsdb.interfaces.storage.fs import RESOURCE_GROUP
from mindsdb.interfaces.storage.json import get_json_storage  # noqa
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._temp_dir = tempfile.TemporaryDirectory(prefix='lightwood_handler_test_')
        os.environ['MINDSDB_STORAGE_DIR'] = os.environ.get('MINDSDB_STORAGE_DIR', cls._temp_dir.name)
        os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'
        # config
        config = {}
        # TODO run on own database
        fdi, cfg_file = tempfile.mkstemp(prefix='mindsdb_conf_')

        with os.fdopen(fdi, 'w') as fd:
            json.dump(config, fd)

        os.environ['MINDSDB_CONFIG_PATH'] = cfg_file

        db.init()
        migrate.migrate_to_head()

    @classmethod
    def tearDownClass(cls):
        if cls._temp_dir:
            try:
                cls._temp_dir.cleanup()
            except NotADirectoryError as e:
                logger.warning('Failed to cleanup temporary directory %s: %s', cls._temp_dir.name, str(e))
            except Exception as e:
                raise e

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

    def test_2_resource_independent(self):
        storage_1 = get_json_storage(1, resource_group=RESOURCE_GROUP.INTEGRATION)
        storage_1['x'] = {'y': 1}
        assert storage_1['x']['y'] == 1

        storage_2 = get_json_storage(1, resource_group=RESOURCE_GROUP.TAB)
        assert storage_2['x'] is None
        storage_2['x'] = {'y': 2}
        assert storage_1['x']['y'] != storage_2['x']['y']


if __name__ == '__main__':
    unittest.main()
