import shutil
import os
from mindsdb.interfaces.state.config import Config
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
except Exception as e:
    # Only required for remote storage on s3
    pass

class StorageEngine():
    def __init__(self, config, location='local'  ):
        self.config = Config(config)
        self.location = location
        if self.location == 'local':
            pass
            os.makedirs(self.tmp_prefix, mode=0o777, exist_ok=True)
        elif self.location == 's3':
            self.s3 = boto3.client('s3')
            self.bucket = os.environ.get('MINDSDB_S3_BUCKET', 'mindsdb-cloud-storage-v1')
        else:
            raise Exception('Location: ' + self.location + 'not supported')


    def _put(self, filename, remote_name, local_path):
        if self.location == 'local':
            shutil.make_archive(f'{remote_name}.zip', 'gztar',root_dir=local_path, base_dir=filename)
        elif self.location == 's3':
            self.s3()
            s3.upload_file('/tmp/hello.txt', 'mybucket', 'hello.txt')

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
