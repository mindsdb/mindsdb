import os
import json
import time
import tempfile
import threading
from pathlib import Path
from typing import Generator

import psutil

from mindsdb.utilities import log

logger = log.getLogger(__name__)


def get_tmp_dir() -> Path:
    return Path(tempfile.gettempdir()).joinpath("mindsdb")


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
    p = get_tmp_dir().joinpath(f"processes/{folder}/")
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
    p = get_tmp_dir().joinpath(f"processes/{folder}/")
    p.mkdir(parents=True, exist_ok=True)
    mark = f"{os.getpid()}-{threading.get_native_id()}-{mark}"
    p.joinpath(mark).touch()
    return mark


def delete_process_mark(folder: str = "learn", mark: str | None = None):
    if mark is None:
        mark = _get_process_mark_id()
    p = get_tmp_dir().joinpath(f"processes/{folder}/").joinpath(mark)
    if p.exists():
        p.unlink()


def clean_process_marks():
    """delete all existing processes marks"""
    logger.debug("Deleting PIDs..")
    p = get_tmp_dir().joinpath("processes/")
    if p.exists() is False:
        return
    for path in p.iterdir():
        if path.is_dir() is False:
            return
        for file in path.iterdir():
            file.unlink()


def get_processes_dir_files_generator() -> Generator[tuple[Path, int, int], None, None]:
    """Get files from processes dir

    Yields:
        tuple(Path, int, int): file object, process id and thread id
    """
    p = get_tmp_dir().joinpath("processes/")
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


def clean_unlinked_process_marks() -> list[int]:
    """delete marks that does not have corresponded processes/threads

    Returns:
        list[int]: list with ids of unexisting processes
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
                logger.warning(f"We have mark for process/thread {process_id}/{thread_id} but it does not exists")
                deleted_pids.append(process_id)
                file.unlink()

        except psutil.AccessDenied:
            logger.warning(f"access to {process_id} denied")
            continue

        except psutil.NoSuchProcess:
            logger.warning(f"We have mark for process/thread {process_id}/{thread_id} but it does not exists")
            deleted_pids.append(process_id)
            file.unlink()
    return deleted_pids


def create_pid_file(config):
    """
    Create mindsdb process pid file. Check if previous process exists and is running
    """

    if os.environ.get("USE_PIDFILE") != "1":
        return

    p = get_tmp_dir()
    p.mkdir(parents=True, exist_ok=True)
    pid_file = p.joinpath("pid")
    if pid_file.exists():
        # if process exists raise exception
        pid_file_data_str = pid_file.read_text().strip()
        pid = None
        try:
            pid_file_data = json.loads(pid_file_data_str)
            pid = pid_file_data.get("pid")
        except json.JSONDecodeError:
            # is it just pid number (old approach)?
            try:
                pid = int(pid_file_data_str)
            except Exception:
                pass
            logger.warning(f"Found existing PID file {pid_file} but it is not a valid JSON, removing")

        if pid is not None:
            try:
                psutil.Process(int(pid))
                raise Exception(f"Found PID file with existing process: {pid} {pid_file}")
            except (psutil.Error, ValueError):
                pass
            logger.warning(f"Found existing PID file {pid_file}({pid}), removing")

        pid_file.unlink()

    pid_file_data_str = json.dumps({
        "pid": os.getpid(),
        "http_host": config.get("api", {}).get("http", {}).get("host"),
        "http_port": config.get("api", {}).get("http", {}).get("port"),
        "http_auth_enabled": config.get("auth", {}).get("http_auth_enabled"),
        "username": config.get("auth", {}).get("username"),
        "password": config.get("auth", {}).get("password"),
    })
    pid_file.write_text(pid_file_data_str)


def delete_pid_file():
    """
    Remove existing process pid file if it matches current process
    """

    if os.environ.get("USE_PIDFILE") != "1":
        return

    pid_file = get_tmp_dir().joinpath("pid")

    if not pid_file.exists():
        return

    pid_file_data_str = pid_file.read_text().strip()
    try:
        pid_file_data = json.loads(pid_file_data_str)
        pid = pid_file_data.get("pid")
    except json.JSONDecodeError:
        logger.warning(f"Found existing PID file {pid_file} but it is not a valid JSON")
    else:
        if str(pid) != str(os.getpid()):
            logger.warning(f"Process id in PID file ({pid_file}) doesn't match mindsdb pid")
            return

    pid_file.unlink()


def __is_within_directory(directory, target):
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    prefix = os.path.commonprefix([abs_directory, abs_target])
    return prefix == abs_directory


def safe_extract(tarfile, path=".", members=None, *, numeric_owner=False):
    # for py >= 3.12
    if hasattr(tarfile, "data_filter"):
        tarfile.extractall(path, members=members, numeric_owner=numeric_owner, filter="data")
        return

    # for py < 3.12
    for member in tarfile.getmembers():
        member_path = os.path.join(path, member.name)
        if not __is_within_directory(path, member_path):
            raise Exception("Attempted Path Traversal in Tar File")
    tarfile.extractall(path, members=members, numeric_owner=numeric_owner)
