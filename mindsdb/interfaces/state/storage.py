import shutil
import os
from mindsdb.interfaces.state.config import Config
try:
    import boto3
except Exception as e:
    # Only required for remote storage on s3
    pass

class StorageEngine():
    def __init__(self, config):
        self.config = Config(config)
        self.location = self.config['permanent_storage']['location']
        if self.location == 'local':
            pass
        elif self.location == 's3':
            self.s3 = boto3.client('s3')
            self.bucket = self.config['permanent_storage']['bucket']
        else:
            raise Exception('Location: ' + self.location + 'not supported')


    def put(self, filename, remote_name, local_path):
        print(f'\n\n\n\n PUTTING IN LOCATION: {self.location} with name {remote_name} \n\n\n\n\n')
        if self.location == 'local':
            pass
        elif self.location == 's3':
            print('\n\n1:\n\n', self.bucket, filename, remote_name, local_path)
            # NOTE: This `make_archive` function is implemente poorly and will create an empty archive file even if the file/dir to be archived doesn't exist or for some other reason can't be archived
            shutil.make_archive(os.path.join(local_path, remote_name), 'gztar',root_dir=local_path, base_dir=filename)
            self.s3.upload_file(os.path.join(local_path, f'{remote_name}.tar.gz'), self.bucket, f'{remote_name}.tar.gz')


    def get(self, remote_name, local_path):
        print(f'\n\n\n\n GETTING FROM LOCATION: {self.location} with name {remote_name} \n\n\n\n\n')
        if self.location == 'local':
            pass
        elif self.location == 's3':
            remote_ziped_name = f'{remote_name}.tar.gz'
            print('\n\n2:\n\n', self.bucket, remote_ziped_name, os.path.join(local_path, remote_ziped_name))
            self.s3.download_file(self.bucket, remote_ziped_name, os.path.join(local_path, remote_ziped_name))
            shutil.unpack_archive(os.path.join(local_path, remote_ziped_name))

    def delete(self, remote_name):
        if self.location == 'local':
            pass
        elif self.location == 's3':
            self.s3.delete_object(Bucket=self.bucket, Key=remote_name)
