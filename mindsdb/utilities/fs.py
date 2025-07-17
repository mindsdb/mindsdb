import os
import time
import tempfile
import threading
from pathlib import Path
from typing import Optional, List, Tuple

import psutil

from mindsdb.utilities import log

logger = log.getLogger(__name__)


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
    p = Path(tempfile.gettempdir()).joinpath(f"mindsdb/processes/{folder}/")
    p.mkdir(parents=True, exist_ok=True)
    mark = f"{os.getpid()}-{threading.get_native_id()}-{mark}"
    p.joinpath(mark).touch()
    return mark


def delete_process_mark(folder: str = "learn", mark: Optional[str] = None):
    if mark is None:
        mark = _get_process_mark_id()
    p = (
        Path(tempfile.gettempdir())
        .joinpath(f"mindsdb/processes/{folder}/")
        .joinpath(mark)
    )
    if p.exists():
        p.unlink()


def clean_process_marks():
    """delete all existing processes marks"""
    logger.debug("Deleting PIDs..")
    p = Path(tempfile.gettempdir()).joinpath("mindsdb/processes/")
    if p.exists() is False:
        return
    for path in p.iterdir():
        if path.is_dir() is False:
            return
        for file in path.iterdir():
            file.unlink()


def get_processes_dir_files_generator() -> Tuple[Path, int, int]:
    """Get files from processes dir

    Yields:
        Tuple[Path, int, int]: file object, process is and thread id
    """
    p = Path(tempfile.gettempdir()).joinpath("mindsdb/processes/")
    if p.exists() is False:
        return
    for path in p.iterdir():
        if path.is_dir() is False:
            continue
        for file in path.iterdir():
            parts = file.name.split("-")
            process_id = int(parts[0])
            thread_id = int(parts[1])
            yield file, process_id, thread_id


def clean_unlinked_process_marks() -> List[int]:
    """delete marks that does not have corresponded processes/threads

    Returns:
        List[int]: list with ids of unexisting processes
    """
    deleted_pids = []

    for file, process_id, thread_id in get_processes_dir_files_generator():
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
                deleted_pids.append(process_id)
                file.unlink()

        except psutil.AccessDenied:
            logger.warning(f"access to {process_id} denied")

            continue

        except psutil.NoSuchProcess:
            logger.warning(
                f"We have mark for process/thread {process_id}/{thread_id} but it does not exists"
            )
            deleted_pids.append(process_id)
            file.unlink()
    return deleted_pids


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
