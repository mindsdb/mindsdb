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


    def put(self, filename, remote_name, local_path):
        if self.location == 'local':
            pass
        elif self.location == 's3':
            remote_ziped_name = f'{remote_name}.zip'
            shutil.make_archive(remote_ziped_name, 'gztar',root_dir=local_path, base_dir=filename)
            self.s3.upload_file(os.path.join(local_path, remote_ziped_name), self.bucket, remote_ziped_name)


    def get(self, remote_name, local_path):
        if self.location == 'local':
            shutil.unpack_archive()
        elif self.location == 's3':
            remote_ziped_name = f'{remote_name}.zip'
            self.s3.download_file(self.bucket, remote_ziped_name, os.path.join(local_path, remote_ziped_name))
            shtuil.unpack_archive(os.path.join(local_path, remote_ziped_name))

    def del(self, remote_name):
        if self.location == 'local':
            pass
        elif self.location == 's3':
            self.s3.delete_object(self.bucket, remote_name)
