import os
import shutil
import hashlib
from abc import ABC, abstractmethod

from checksumdir import dirhash
try:
    import boto3
except Exception:
    # Only required for remote storage on s3
    pass

from mindsdb.utilities.config import Config


def copy(src, dst):
    if os.path.isdir(src):
        if os.path.exists(dst):
            if dirhash(src) == dirhash(dst):
                return
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)
    else:
        if os.path.exists(dst):
            if hashlib.md5(open(src, 'rb').read()).hexdigest() == hashlib.md5(open(dst, 'rb').read()).hexdigest():
                return
        try:
            os.remove(dst)
        except Exception:
            pass
        shutil.copy2(src, dst)


class BaseFSStore(ABC):
    def __init__(self):
        self.config = Config()
        self.storage = self.config['paths']['storage']

    @abstractmethod
    def get(self, name):
        pass

    @abstractmethod
    def put(self, name):
        pass

    @abstractmethod
    def delete(self, name):
        pass


class LocalFSStore(BaseFSStore):
    def __init__(self):
        super().__init__()

    def get(self, filename, remote_name, local_path):
        copy(
            os.path.join(self.storage, remote_name),
            os.path.join(local_path, filename)
        )

    def put(self, filename, remote_name, local_path):
        print('From: ', os.path.join(local_path, filename))
        print('To: ', os.path.join(self.storage, remote_name))
        copy(
            os.path.join(local_path, filename),
            os.path.join(self.storage, remote_name)
        )

    def delete(self, remote_name):
        pass


class S3FSStore(BaseFSStore):
    def __init__(self):
        super().__init__()
        if 's3_credentials' in self.config['permanent_storage']:
            self.s3 = boto3.client('s3', **self.config['permanent_storage']['s3_credentials'])
        else:
            self.s3 = boto3.client('s3')
        self.bucket = self.config['permanent_storage']['bucket']

    def get(self, filename, remote_name, local_path):
        remote_ziped_name = f'{remote_name}.tar.gz'
        local_ziped_name = f'{filename}.tar.gz'
        local_ziped_path = os.path.join(local_path, local_ziped_name)
        self.s3.download_file(self.bucket, remote_ziped_name, local_ziped_path)
        shutil.unpack_archive(local_ziped_path, local_path)
        os.system(f'chmod -R 777 {local_path}')
        os.remove(local_ziped_path)

    def put(self, filename, remote_name, local_path):
        # NOTE: This `make_archive` function is implemente poorly and will create an empty archive file even if
        # the file/dir to be archived doesn't exist or for some other reason can't be archived
        shutil.make_archive(
            os.path.join(local_path, remote_name),
            'gztar',
            root_dir=local_path,
            base_dir=filename
        )
        self.s3.upload_file(
            os.path.join(local_path, f'{remote_name}.tar.gz'),
            self.bucket,
            f'{remote_name}.tar.gz'
        )
        os.remove(os.path.join(local_path, remote_name + '.tar.gz'))

    def delete(self, remote_name):
        self.s3.delete_object(Bucket=self.bucket, Key=remote_name)


storage_location = Config().config['permanent_storage']['location']
if storage_location == 'local':
    FsStore = LocalFSStore
elif storage_location == 's3':
    FsStore = S3FSStore
else:
    raise Exception(f"Location: '{storage_location}' not supported")
