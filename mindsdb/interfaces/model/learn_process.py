import os
import logging
import tempfile
from pathlib import Path
import lightwood
import mindsdb_datasources

import torch.multiprocessing as mp

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.config import Config
from mindsdb.utilities.fs import create_process_mark, delete_process_mark


ctx = mp.get_context('spawn')


def create_learn_mark():
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath('mindsdb/learn_processes/')
        p.mkdir(parents=True, exist_ok=True)
        p.joinpath(f'{os.getpid()}').touch()


def delete_learn_mark():
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath('mindsdb/learn_processes/').joinpath(f'{os.getpid()}')
        if p.exists():
            p.unlink()
