import os
import io
import shutil
import filecmp
import tarfile
import hashlib
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Union, Optional
from dataclasses import dataclass
from datetime import datetime
import threading

if os.name == 'posix':
    import fcntl

import psutil

from mindsdb.utilities.config import Config

if Config()['permanent_storage']['location'] == 's3':
    import boto3
    from botocore.exceptions import ClientError as S3ClientError
else:
    S3ClientError = FileNotFoundError

from mindsdb.utilities.context import context as ctx
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities import log
from mindsdb.utilities.fs import safe_extract

logger = log.getLogger(__name__)


@dataclass(frozen=True)
class RESOURCE_GROUP:
    PREDICTOR = 'predictor'
    INTEGRATION = 'integration'
    TAB = 'tab'
    SYSTEM = 'system'


RESOURCE_GROUP = RESOURCE_GROUP()


DIR_LOCK_FILE_NAME = 'dir.lock'
DIR_LAST_MODIFIED_FILE_NAME = 'last_modified.txt'
SERVICE_FILES_NAMES = (DIR_LOCK_FILE_NAME, DIR_LAST_MODIFIED_FILE_NAME)


def compare_recursive(comparison: filecmp.dircmp) -> bool:
    """Check output of dircmp and return True if the directories do not differ

    Args:
        comparison (filecmp.dircmp): dirs comparison

    Returns:
        bool: True if dirs do not differ
    """
    if comparison.left_only or comparison.right_only or comparison.diff_files:
        return False
    for sub_comparison in comparison.subdirs.values():
        if compare_recursive(sub_comparison) is False:
            return False
    return True


def compare_directories(dir1: str, dir2: str) -> bool:
    """Compare two directories

    Args:
        dir1 (str): dir to compare
        dir2 (str): dir to compare

    Returns:
        bool: True if dirs do not differ
    """
    dcmp = filecmp.dircmp(dir1, dir2)
    return compare_recursive(dcmp)


def copy(src, dst):
    if os.path.isdir(src):
        if os.path.exists(dst):
            if compare_directories(src, dst):
                return
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst, dirs_exist_ok=True)
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


def get_dir_size(path: str):
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total


class AbsentFSStore(BaseFSStore):
    """Storage class that does not store anything. It is just a dummy.
    """
    def get(self, *args, **kwargs):
        pass

    def put(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass


class LocalFSStore(BaseFSStore):
    """Storage that stores files locally
    """

    def __init__(self):
        super().__init__()

    def get(self, local_name, base_dir):
        remote_name = local_name
        src = os.path.join(self.storage, remote_name)
        dest = os.path.join(base_dir, local_name)
        if not os.path.exists(dest) or get_dir_size(src) != get_dir_size(dest):
            copy(src, dest)

    def put(self, local_name, base_dir, compression_level=9):
        remote_name = local_name
        copy(
            os.path.join(base_dir, local_name),
            os.path.join(self.storage, remote_name)
        )

    def delete(self, remote_name):
        path = Path(self.storage).joinpath(remote_name)
        try:
            if path.is_file():
                path.unlink()
            else:
                shutil.rmtree(path)
        except FileNotFoundError:
            pass


class FileLock:
    """ file lock to make safe concurrent access to directory
        works as context
    """

    @staticmethod
    def lock_folder_path(relative_path: Path) -> Path:
        """ Args:
                relative_path (Path): path to resource directory relative to storage root

            Returns:
                Path: abs path to folder with lock file
        """
        config = Config()
        root_storage_path = Path(config.paths['root'])
        return config.paths['locks'] / relative_path.relative_to(root_storage_path)

    def __init__(self, relative_path: Path, mode: str = 'w'):
        """ Args:
                relative_path (Path): path to resource directory relative to storage root
                mode (str): lock for read (r) or write (w)
        """
        if os.name != 'posix':
            return

        self._local_path = FileLock.lock_folder_path(relative_path)
        self._lock_file_name = DIR_LOCK_FILE_NAME
        self._lock_file_path = self._local_path / self._lock_file_name
        self._mode = fcntl.LOCK_EX if mode == 'w' else fcntl.LOCK_SH

        if self._lock_file_path.is_file() is False:
            self._local_path.mkdir(parents=True, exist_ok=True)
            try:
                self._lock_file_path.write_text('')
            except Exception:
                pass

    def __enter__(self):
        if os.name != 'posix':
            return

        try:
            # On at least some systems, LOCK_EX can only be used if the file
            # descriptor refers to a file opened for writing.
            self._lock_fd = os.open(self._lock_file_path, os.O_RDWR | os.O_CREAT)
            fcntl.lockf(self._lock_fd, self._mode | fcntl.LOCK_NB)
        except (ValueError, FileNotFoundError):
            # file probably was deleted between open and lock
            logger.error(f'Cant accure lock on {self._local_path}')
            raise FileNotFoundError
        except BlockingIOError:
            logger.error(f'Directory is locked by another process: {self._local_path}')
            fcntl.lockf(self._lock_fd, self._mode)

    def __exit__(self, exc_type, exc_value, traceback):
        if os.name != 'posix':
            return

        try:
            fcntl.lockf(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
        except Exception:
            pass


class S3FSStore(BaseFSStore):
    """Storage that stores files in amazon s3
    """

    dt_format = '%d.%m.%y %H:%M:%S.%f'

    def __init__(self):
        super().__init__()
        if 's3_credentials' in self.config['permanent_storage']:
            self.s3 = boto3.client('s3', **self.config['permanent_storage']['s3_credentials'])
        else:
            self.s3 = boto3.client('s3')
        self.bucket = self.config['permanent_storage']['bucket']
        self._thread_lock = threading.Lock()

    def _get_remote_last_modified(self, object_name: str) -> datetime:
        """ get time when object was created/modified

            Args:
                object_name (str): name if file in bucket

            Returns:
                datetime
        """
        last_modified = self.s3.get_object_attributes(
            Bucket=self.bucket,
            Key=object_name,
            ObjectAttributes=['Checksum']
        )['LastModified']
        last_modified = last_modified.replace(tzinfo=None)
        return last_modified

    @profiler.profile()
    def _get_local_last_modified(self, base_dir: str, local_name: str) -> datetime:
        """ get 'last_modified' that saved locally

            Args:
                base_dir (str): path to base folder
                local_name (str): folder name

            Returns:
                datetime | None
        """
        last_modified_file_path = Path(base_dir) / local_name / DIR_LAST_MODIFIED_FILE_NAME
        if last_modified_file_path.is_file() is False:
            return None
        try:
            last_modified_text = last_modified_file_path.read_text()
            last_modified_datetime = datetime.strptime(last_modified_text, self.dt_format)
        except Exception:
            return None
        return last_modified_datetime

    @profiler.profile()
    def _save_local_last_modified(self, base_dir: str, local_name: str, last_modified: datetime):
        """ Save 'last_modified' to local folder

            Args:
                base_dir (str): path to base folder
                local_name (str): folder name
                last_modified (datetime)
        """
        last_modified_file_path = Path(base_dir) / local_name / DIR_LAST_MODIFIED_FILE_NAME
        last_modified_text = last_modified.strftime(self.dt_format)
        last_modified_file_path.write_text(last_modified_text)

    @profiler.profile()
    def _download(self, base_dir: str, remote_ziped_name: str,
                  local_ziped_path: str, last_modified: datetime = None):
        """ download file to s3 and unarchive it

            Args:
                base_dir (str)
                remote_ziped_name (str)
                local_ziped_path (str)
                last_modified (datetime, optional)
        """
        os.makedirs(base_dir, exist_ok=True)

        remote_size = self.s3.get_object_attributes(
            Bucket=self.bucket,
            Key=remote_ziped_name,
            ObjectAttributes=['ObjectSize']
        )['ObjectSize']
        if (remote_size * 2) > psutil.virtual_memory().available:
            fh = io.BytesIO()
            self.s3.download_fileobj(self.bucket, remote_ziped_name, fh)
            with tarfile.open(fileobj=fh) as tar:
                safe_extract(tar, path=base_dir)
        else:
            self.s3.download_file(self.bucket, remote_ziped_name, local_ziped_path)
            shutil.unpack_archive(local_ziped_path, base_dir)
            os.remove(local_ziped_path)

        # os.system(f'chmod -R 777 {base_dir}')

        if last_modified is None:
            last_modified = self._get_remote_last_modified(remote_ziped_name)
        self._save_local_last_modified(
            base_dir,
            remote_ziped_name.replace('.tar.gz', ''),
            last_modified
        )

    @profiler.profile()
    def get(self, local_name, base_dir):
        remote_name = local_name
        remote_ziped_name = f'{remote_name}.tar.gz'
        local_ziped_name = f'{local_name}.tar.gz'
        local_ziped_path = os.path.join(base_dir, local_ziped_name)

        folder_path = Path(base_dir) / local_name
        with FileLock(folder_path, mode='r'):
            local_last_modified = self._get_local_last_modified(base_dir, local_name)
            remote_last_modified = self._get_remote_last_modified(remote_ziped_name)
            if (
                local_last_modified is not None
                and local_last_modified == remote_last_modified
            ):
                return

        with FileLock(folder_path, mode='w'):
            self._download(
                base_dir,
                remote_ziped_name,
                local_ziped_path,
                last_modified=remote_last_modified
            )

    @profiler.profile()
    def put(self, local_name, base_dir, compression_level=9):
        # NOTE: This `make_archive` function is implemente poorly and will create an empty archive file even if
        # the file/dir to be archived doesn't exist or for some other reason can't be archived
        remote_name = local_name
        remote_zipped_name = f'{remote_name}.tar.gz'

        dir_path = Path(base_dir) / remote_name
        dir_size = sum(f.stat().st_size for f in dir_path.glob('**/*') if f.is_file())
        if (dir_size * 2) < psutil.virtual_memory().available:
            old_cwd = os.getcwd()
            fh = io.BytesIO()
            with self._thread_lock:
                os.chdir(base_dir)
                with tarfile.open(fileobj=fh, mode='w:gz', compresslevel=compression_level) as tar:
                    for path in dir_path.iterdir():
                        if path.is_file() and path.name in SERVICE_FILES_NAMES:
                            continue
                        tar.add(path.relative_to(base_dir))
                os.chdir(old_cwd)
            fh.seek(0)

            self.s3.upload_fileobj(
                fh,
                self.bucket,
                remote_zipped_name
            )
        else:
            shutil.make_archive(
                os.path.join(base_dir, remote_name),
                'gztar',
                root_dir=base_dir,
                base_dir=local_name
            )

            self.s3.upload_file(
                os.path.join(base_dir, remote_zipped_name),
                self.bucket,
                remote_zipped_name
            )
            os.remove(os.path.join(base_dir, remote_zipped_name))

        last_modified = self._get_remote_last_modified(remote_zipped_name)
        self._save_local_last_modified(base_dir, local_name, last_modified)

    @profiler.profile()
    def delete(self, remote_name):
        self.s3.delete_object(Bucket=self.bucket, Key=remote_name)


def FsStore():
    storage_location = Config()['permanent_storage']['location']
    if storage_location == 'absent':
        return AbsentFSStore()
    if storage_location == 'local':
        return LocalFSStore()
    if storage_location == 's3':
        return S3FSStore()
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

    @profiler.profile()
    def push(self, compression_level: int = 9):
        with FileLock(self.folder_path, mode='r'):
            self._push_no_lock(compression_level=compression_level)

    @profiler.profile()
    def _push_no_lock(self, compression_level: int = 9):
        self.fs_store.put(
            str(self.folder_name),
            str(self.resource_group_path),
            compression_level=compression_level
        )

    @profiler.profile()
    def push_path(self, path, compression_level: int = 9):
        # TODO implement push per element
        self.push(compression_level=compression_level)

    @profiler.profile()
    def pull(self):
        try:
            self.fs_store.get(
                str(self.folder_name),
                str(self.resource_group_path)
            )
        except (FileNotFoundError, S3ClientError):
            pass

    @profiler.profile()
    def pull_path(self, path):
        # TODO implement pull per element
        self.pull()

    @profiler.profile()
    def file_set(self, name, content):
        if self.sync is True:
            self.pull()

        with FileLock(self.folder_path, mode='w'):

            dest_abs_path = self.folder_path / name

            with open(dest_abs_path, 'wb') as fd:
                fd.write(content)

            if self.sync is True:
                self._push_no_lock()

    @profiler.profile()
    def file_get(self, name):
        if self.sync is True:
            self.pull()
        dest_abs_path = self.folder_path / name
        with FileLock(self.folder_path, mode='r'):
            with open(dest_abs_path, 'rb') as fd:
                return fd.read()

    @profiler.profile()
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
        with FileLock(self.folder_path, mode='w'):

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
                self._push_no_lock()

    @profiler.profile()
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

        with FileLock(self.folder_path, mode='r'):
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

    def delete(self, relative_path: Union[str, Path] = '.'):
        path = (self.folder_path / relative_path).resolve()
        if isinstance(relative_path, str):
            relative_path = Path(relative_path)

        if relative_path.is_absolute():
            raise TypeError('FSStorage.delete() got absolute path as argument')

        # complete removal
        if path == self.folder_path.resolve():
            with FileLock(self.folder_path, mode='w'):
                self.fs_store.delete(self.folder_name)
                # NOTE on some fs .rmtree is not working if any file is open
                shutil.rmtree(str(self.folder_path))

            # region del file lock
            lock_folder_path = FileLock.lock_folder_path(self.folder_path)
            try:
                shutil.rmtree(lock_folder_path)
            except FileNotFoundError:
                logger.warning('Tried to delete file not found: %s', lock_folder_path)
            except Exception as e:
                raise e
            # endregion
            return

        if self.sync is True:
            self.pull()

        with FileLock(self.folder_path, mode='w'):
            if path.exists() is False:
                raise Exception('Path does not exists')

            if path.is_file():
                path.unlink()
            else:
                path.rmdir()

            if self.sync is True:
                self._push_no_lock()


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
