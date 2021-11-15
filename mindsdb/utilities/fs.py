import os
import time
import tempfile
import threading
from pathlib import Path
from typing import Optional

from appdirs import user_data_dir


def create_directory(path):
    path = Path(path)
    path.mkdir(mode=0o777, exist_ok=True, parents=True)


def get_root_path():
    mindsdb_path = user_data_dir('mindsdb', 'mindsdb')
    return os.path.join(mindsdb_path, 'var/')


def get_or_create_data_dir():
    data_dir = user_data_dir('mindsdb', 'mindsdb')
    mindsdb_data_dir = os.path.join(data_dir, 'var/')

    if os.path.exists(mindsdb_data_dir) is False:
        create_directory(mindsdb_data_dir)

    try:
        assert os.path.exists(mindsdb_data_dir)
        assert os.access(mindsdb_data_dir, os.W_OK) is True
    except Exception:
        raise Exception('MindsDB storage directory does not exist and could not be created')

    return mindsdb_data_dir


def create_dirs_recursive(path):
    if isinstance(path, dict):
        for p in path.values():
            create_dirs_recursive(p)
    elif isinstance(path, str):
        create_directory(path)
    else:
        raise ValueError(f'Wrong path: {path}')


def _get_process_mark_id(unified: bool = False) -> str:
    ''' Creates a text that can be used to identify process+thread
        Args:
            unified: bool, if True then result will be same for same process+thread
        Returns:
            mark of process+thread
    '''
    mark = f'{os.getpid()}-{threading.get_ident()}'
    if unified is True:
        return mark
    return f"{mark}-{str(time.time()).replace('.', '')}"


def create_process_mark(folder='learn'):
    mark = None
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath(f'mindsdb/processes/{folder}/')
        p.mkdir(parents=True, exist_ok=True)
        mark = _get_process_mark_id()
        p.joinpath(mark).touch()
    return mark


def delete_process_mark(folder: str = 'learn', mark: Optional[str] = None):
    if mark is None:
        mark = _get_process_mark_id()
    if os.name == 'posix':
        p = (
            Path(tempfile.gettempdir())
            .joinpath(f'mindsdb/processes/{folder}/')
            .joinpath(mark)
        )
        if p.exists():
            p.unlink()
