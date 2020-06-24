import inspect
import os
from pathlib import Path
import traceback


def create_directory(path):
    path = Path(path)
    path.mkdir(mode=0o777, exist_ok=True, parents=True)

def get_paths():
    this_file_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
    mindsdb_path = os.path.abspath(Path(this_file_path).parent.parent.parent)

    return [(f'{mindsdb_path}/etc/', f'{mindsdb_path}/var/predictors',f'{mindsdb_path}/var/datastore'),('/etc/mindsdb', '/var/lib/mindsdb/predictors','/var/lib/mindsdb/datastore'),('~/.local/etc/mindsdb','~/.local/var/lib/mindsdb/predictors','~/.local/var/lib/mindsdb/datastore')]

def get_or_create_dir_struct():
    for tup in get_paths():
        try:
            for dir in tup:
                assert(os.path.exists(dir))
                os.access(dir, os.W_OK)
            return tup[0], tup[1], tup[2]
        except Exception as e:
            pass

    for tup in get_paths():
        try:
            for dir in tup:
                create_directory(dir)
                os.access(dir, os.W_OK)
            return tup[0], tup[1], tup[2]
        except Exception as e:
            pass

    raise Exception(f'MindsDB storage directory: {path} does not exist and could not be created, trying another directory')
