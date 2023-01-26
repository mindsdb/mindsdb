import os
import shutil
import hashlib
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Union, Optional
from dataclasses import dataclass

from checksumdir import dirhash
try:
    import boto3
except Exception:
    # Only required for remote storage on s3
    pass

from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx


@dataclass(frozen=True)
class RESOURCE_GROUP:
    PREDICTOR = 'predictor'
    INTEGRATION = 'integration'
    TAB = 'tab'


RESOURCE_GROUP = RESOURCE_GROUP()


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
        os.makedirs(base_dir, exist_ok=True)
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


def FsStore():
    storage_location = Config()['permanent_storage']['location']
    if storage_location == 'local':
        return LocalFSStore()
    elif storage_location == 's3':
        return S3FSStore()
    else:
        raise Exception(f"Location: '{storage_location}' not supported")


class FileStorage:
    def __init__(self, resource_group: str, resource_id: int,
                 root_dir: str = 'content', sync: bool = True):
        """
            Args:
                resource_group (str)
                resource_id (int)
                root_dir (str)
                sync (bool)
        """

        self.resource_group = resource_group
        self.resource_id = resource_id
        self.root_dir = root_dir
        self.sync = sync

        self.folder_name = f'{resource_group}_{ctx.company_id}_{resource_id}'

        config = Config()
        self.fs_store = FsStore()
        self.content_path = Path(config['paths'][root_dir])
        self.resource_group_path = self.content_path / resource_group
        self.folder_path = self.resource_group_path / self.folder_name
        if self.folder_path.exists() is False:
            self.folder_path.mkdir(parents=True, exist_ok=True)

    def push(self):
        self.fs_store.put(str(self.folder_name), str(self.resource_group_path))

    def push_path(self, path):
        self.fs_store.put(os.path.join(self.folder_name, path), str(self.resource_group_path))

    def pull(self):
        try:
            self.fs_store.get(str(self.folder_name), str(self.resource_group_path))
        except Exception:
            pass

    def pull_path(self, path, update=True):
        if update is False:
            # not pull from source if object is exists
            if os.path.exists(self.resource_group_path / self.folder_name / path):
                return
        try:
            # TODO not sync if not changed?
            self.fs_store.get(os.path.join(self.folder_name, path), str(self.resource_group_path))
        except Exception:
            pass

    def file_set(self, name, content):
        if self.sync is True:
            self.pull()

        dest_abs_path = self.folder_path / name

        with open(dest_abs_path, 'wb') as fd:
            fd.write(content)

        if self.sync is True:
            self.push()

    def file_get(self, name):

        if self.sync is True:
            self.pull()

        dest_abs_path = self.folder_path / name

        with open(dest_abs_path, 'rb') as fd:
            return fd.read()

    def add(self, path: Union[str, Path], dest_rel_path: Optional[Union[str, Path]] = None):
        """Copy file/folder to persist storage

        Examples:
            Copy file 'args.json' to '{storage}/args.json'
            >>> fs.add('/path/args.json')

            Copy file 'args.json' to '{storage}/folder/opts.json'
            >>> fs.add('/path/args.json', 'folder/opts.json')

            Copy folder 'folder' to '{storage}/folder'
            >>> fs.add('/path/folder')

            Copy folder 'folder' to '{storage}/path/folder'
            >>> fs.add('/path/folder', 'path/folder')

        Args:
            path (Union[str, Path]): path to the resource
            dest_rel_path (Optional[Union[str, Path]]): relative path in storage to file or folder
        """
        if self.sync is True:
            self.pull()

        path = Path(path)
        if isinstance(dest_rel_path, str):
            dest_rel_path = Path(dest_rel_path)

        if dest_rel_path is None:
            dest_abs_path = self.folder_path / path.name
        else:
            dest_abs_path = self.folder_path / dest_rel_path

        copy(
            str(path),
            str(dest_abs_path)
        )

        if self.sync is True:
            self.push()

    def get_path(self, relative_path: Union[str, Path]) -> Path:
        """ Return path to file or folder

        Examples:
            get path to 'opts.json':
            >>> fs.get_path('folder/opts.json')
            ... /path/{storage}/folder/opts.json

        Args:
            relative_path (Union[str, Path]): Path relative to the storage folder

        Returns:
            Path: path to requested file or folder
        """
        if self.sync is True:
            self.pull()

        if isinstance(relative_path, str):
            relative_path = Path(relative_path)
        # relative_path = relative_path.resolve()

        if relative_path.is_absolute():
            raise TypeError('FSStorage.get_path() got absolute path as argument')

        ret_path = self.folder_path / relative_path
        if not ret_path.exists():
            # raise Exception('Path does not exists')
            os.makedirs(ret_path)

        return ret_path

    def delete(self, relative_path: Union[str, Path] = '.') -> Path:
        if isinstance(relative_path, str):
            relative_path = Path(relative_path)

        if relative_path.is_absolute():
            raise TypeError('FSStorage.delete() got absolute path as argument')

        path = (self.folder_path / relative_path).resolve()

        if path == self.folder_path.resolve():
            return self.complete_removal()

        if self.sync is True:
            self.pull()

        if path.exists() is False:
            raise Exception('Path does not exists')

        if path.is_file():
            path.unlink()
        else:
            path.rmdir()

        if self.sync is True:
            self.push()

    def complete_removal(self):
        shutil.rmtree(str(self.folder_path))
        self.fs_store.delete(self.folder_name)


class FileStorageFactory:
    def __init__(self, resource_group: str,
                 root_dir: str = 'content', sync: bool = True):
        self.resource_group = resource_group
        self.root_dir = root_dir
        self.sync = sync

    def __call__(self, resource_id: int):
        return FileStorage(
            resource_group=self.resource_group,
            root_dir=self.root_dir,
            sync=self.sync,
            resource_id=resource_id
        )
