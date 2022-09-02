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
    """Base class for file storage
    """

    def __init__(self):
        self.config = Config()
        self.storage = self.config['paths']['storage']

    @abstractmethod
    def get(self, local_name, base_dir):
        """Copy file/folder from storage to {base_dir}

        Args:
            local_name (str): name of resource (file/folder)
            base_dir (str): path to copy the resource
        """
        pass

    @abstractmethod
    def put(self, local_name, base_dir):
        """Copy file/folder from {base_dir} to storage

        Args:
            local_name (str): name of resource (file/folder)
            base_dir (str): path to folder with the resource
        """
        pass

    @abstractmethod
    def delete(self, remote_name):
        """Delete file/folder from storage

        Args:
            remote_name (str): name of resource
        """
        pass


class LocalFSStore(BaseFSStore):
    """Storage that stores files locally
    """

    def __init__(self):
        super().__init__()

    def get(self, local_name, base_dir):
        remote_name = local_name
        copy(
            os.path.join(self.storage, remote_name),
            os.path.join(base_dir, local_name)
        )

    def put(self, local_name, base_dir):
        remote_name = local_name
        copy(
            os.path.join(base_dir, local_name),
            os.path.join(self.storage, remote_name)
        )

    def delete(self, remote_name):
        pass


class S3FSStore(BaseFSStore):
    """Storage that stores files in amazon s3
    """

    def __init__(self):
        super().__init__()
        if 's3_credentials' in self.config['permanent_storage']:
            self.s3 = boto3.client('s3', **self.config['permanent_storage']['s3_credentials'])
        else:
            self.s3 = boto3.client('s3')
        self.bucket = self.config['permanent_storage']['bucket']

    def get(self, local_name, base_dir):
        remote_name = local_name
        remote_ziped_name = f'{remote_name}.tar.gz'
        local_ziped_name = f'{local_name}.tar.gz'
        local_ziped_path = os.path.join(base_dir, local_ziped_name)
        self.s3.download_file(self.bucket, remote_ziped_name, local_ziped_path)
        shutil.unpack_archive(local_ziped_path, base_dir)
        os.system(f'chmod -R 777 {base_dir}')
        os.remove(local_ziped_path)

    def put(self, local_name, base_dir):
        # NOTE: This `make_archive` function is implemente poorly and will create an empty archive file even if
        # the file/dir to be archived doesn't exist or for some other reason can't be archived
        remote_name = local_name
        shutil.make_archive(
            os.path.join(base_dir, remote_name),
            'gztar',
            root_dir=base_dir,
            base_dir=local_name
        )
        self.s3.upload_file(
            os.path.join(base_dir, f'{remote_name}.tar.gz'),
            self.bucket,
            f'{remote_name}.tar.gz'
        )
        os.remove(os.path.join(base_dir, remote_name + '.tar.gz'))

    def delete(self, remote_name):
        self.s3.delete_object(Bucket=self.bucket, Key=remote_name)


storage_location = Config()['permanent_storage']['location']
if storage_location == 'local':
    FsStore = LocalFSStore
elif storage_location == 's3':
    FsStore = S3FSStore
else:
    raise Exception(f"Location: '{storage_location}' not supported")


class SpecificFSStore:
    def __init__(self, resource_name: str, resource_id: int, company_id=None):
        self.fs_store = FsStore()
        self.folder_name = f'{resource_name}_{resource_id}_{company_id}'
        pass

    def push(self):
        pass

    def pull(self):
        pass

    def add(self, path: str):
        """Copy file/folder to persist storage

        Args:
            path (str): path to the resource
        """
        pass

    def get_path(self, relative_path):
        pass

    # def list(self, relative_path=''):
    #     pass

# class SpecificFSStore:
#     def __init__(self, resource_name: str):
#         self.resource_name = resource_name
#         super().__init__()

#     def get(self):

#         super().get(self, local_name, base_dir), local_name, base_dir
#         pass

#     def put(self, local_name, base_dir):
#         super().put()

#     def delete(self):
#         # , remote_name
#         super().delete()
