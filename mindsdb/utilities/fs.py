import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

import psutil
from appdirs import user_data_dir

from mindsdb.utilities import log

logger = log.getLogger(__name__)


def create_directory(path):
    path = Path(path)
    path.mkdir(mode=0o777, exist_ok=True, parents=True)


def get_root_path():
    mindsdb_path = user_data_dir("mindsdb", "mindsdb")
    return os.path.join(mindsdb_path, "var/")


def get_or_create_data_dir():
    data_dir = user_data_dir("mindsdb", "mindsdb")
    mindsdb_data_dir = os.path.join(data_dir, "var/")

    if os.path.exists(mindsdb_data_dir) is False:
        create_directory(mindsdb_data_dir)

    try:
        assert os.path.exists(mindsdb_data_dir)
        assert os.access(mindsdb_data_dir, os.W_OK) is True
    except Exception:
        raise Exception(
            "MindsDB storage directory does not exist and could not be created"
        )

    return mindsdb_data_dir


def create_dirs_recursive(path):
    if isinstance(path, dict):
        for p in path.values():
            create_dirs_recursive(p)
    elif isinstance(path, str):
        create_directory(path)
    else:
        raise ValueError(f"Wrong path: {path}")


def _get_process_mark_id(unified: bool = False) -> str:
    """Creates a text that can be used to identify process+thread
    Args:
        unified: bool, if True then result will be same for same process+thread
    Returns:
        mark of process+thread
    """
    mark = f"{os.getpid()}-{threading.get_native_id()}"
    if unified is True:
        return mark
    return f"{mark}-{str(time.time()).replace('.', '')}"


def create_process_mark(folder="learn"):
    mark = None
    if os.name == "posix":
        p = Path(tempfile.gettempdir()).joinpath(f"mindsdb/processes/{folder}/")
        p.mkdir(parents=True, exist_ok=True)
        mark = _get_process_mark_id()
        p.joinpath(mark).touch()
    return mark


def set_process_mark(folder: str, mark: str) -> None:
    """touch new file which will be process mark

    Args:
        folder (str): where create the file
        mark (str): file name

    Returns:
        str: process mark
    """
    if os.name != "posix":
        return
    p = Path(tempfile.gettempdir()).joinpath(f"mindsdb/processes/{folder}/")
    p.mkdir(parents=True, exist_ok=True)
    mark = f"{os.getpid()}-{threading.get_native_id()}-{mark}"
    p.joinpath(mark).touch()
    return mark


def delete_process_mark(folder: str = "learn", mark: Optional[str] = None):
    if mark is None:
        mark = _get_process_mark_id()
    if os.name == "posix":
        p = (
            Path(tempfile.gettempdir())
            .joinpath(f"mindsdb/processes/{folder}/")
            .joinpath(mark)
        )
        if p.exists():
            p.unlink()


def clean_process_marks():
    """delete all existing processes marks"""
    if os.name != "posix":
        return

    logger.debug("Deleting PIDs..")
    p = Path(tempfile.gettempdir()).joinpath("mindsdb/processes/")
    if p.exists() is False:
        return
    for path in p.iterdir():
        if path.is_dir() is False:
            return
        for file in path.iterdir():
            file.unlink()


def clean_unlinked_process_marks():
    """delete marks that does not have corresponded processes/threads"""
    if os.name != "posix":
        return

    p = Path(tempfile.gettempdir()).joinpath("mindsdb/processes/")
    if p.exists() is False:
        return
    for path in p.iterdir():
        if path.is_dir() is False:
            return
        for file in path.iterdir():
            parts = file.name.split("-")
            process_id = int(parts[0])
            thread_id = int(parts[1])

            try:
                process = psutil.Process(process_id)
                if process.status() in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                    raise psutil.NoSuchProcess(process_id)

                threads = process.threads()
                try:
                    next(t for t in threads if t.id == thread_id)
                except StopIteration:
                    logger.warning(
                        f"We have mark for process/thread {process_id}/{thread_id} but it does not exists"
                    )
                    file.unlink()

            except psutil.AccessDenied:
                logger.warning(f"access to {process_id} denied")

                continue

            except psutil.NoSuchProcess:
                logger.warning(
                    f"We have mark for process/thread {process_id}/{thread_id} but it does not exists"
                )
                file.unlink()


def __is_within_directory(directory, target):
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    prefix = os.path.commonprefix([abs_directory, abs_target])
    return prefix == abs_directory


def safe_extract(tarfile, path=".", members=None, *, numeric_owner=False):
    # for py >= 3.12
    if hasattr(tarfile, 'data_filter'):
        tarfile.extractall(path, members=members, numeric_owner=numeric_owner, filter='data')
        return

    # for py < 3.12
    for member in tarfile.getmembers():
        member_path = os.path.join(path, member.name)
        if not __is_within_directory(path, member_path):
            raise Exception("Attempted Path Traversal in Tar File")
    tarfile.extractall(path, members=members, numeric_owner=numeric_owner)
