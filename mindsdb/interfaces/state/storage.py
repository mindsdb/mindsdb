import shutil
import os
from mindsdb.interfaces.state.config import Config

class StorageEngine():
    def __init__(self, location='local', connect_data=None):
        self.config = Config()
        self.location = location
        if self.location == 'local':
            self.prefix = os.path.join(self.config.paths['root'],'storage_engine')
            self.tmp_prefix = os.path.join(self.prefix,'tmp')
            os.makedirs(self.tmp_prefix, mode=0o777, exist_ok=True)

    def _put(self, key, path):
        if self.location == 'local':
            shutil.make_archive(f'{key}.zip', 'gztar',root_dir=self.prefix, base_dir=path)
        else:
            raise Exception('Location: ' + self.location + 'not supported')

    def _get(self, key):
        if self.location == 'local':
            shutil.unpack_archive()
        else:
            raise Exception('Location: ' + self.location + 'not supported')

    def _del(self, key):
        if self.location == 'local':
            shutil.rmtree(path, ignore_errors=False, onerror=None)
        else:
            raise Exception('Location: ' + self.location + 'not supported')

    def put_object(self, key, object):
        pass

    def put_fs_node(self, key, path):
        pass

    def get_object(self, key):
        pass

    def get_fs_node(self, key, path):
        pass

    def del_obj(self, key):
        pass

    def del_fs_node(self, key):
        pass
