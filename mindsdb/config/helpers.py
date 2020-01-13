import inspect
import os
from pathlib import Path

from mindsdb.__about__ import __version__


def if_env_else(env_var, else_value):
    """
    return else_value if env_var is not set in environment variables
    :return:
    """
    return else_value if env_var not in os.environ else os.environ[env_var]


def create_directory(path):
    if not os.path.exists(path):
        try:
            print(f'{path} does not exist, creating it now')
            path = Path(path)
            path.mkdir(mode=0o777, exist_ok=True, parents=True)
        except:
            print(traceback.format_exc())
            print(f'MindsDB storage directory: {path} does not exist and could not be created')

def get_and_create_default_storage_path():
    mindsdb_path = os.path.abspath(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))+'/../../')
    path = os.path.abspath(f'{mindsdb_path}/mindsdb_storage/{__version__.replace('.', '_')}')

    create_directory(path)
    if not os.access(path, os.W_OK):
        home = os.path.expanduser('~')
        path = os.path.join(home, '.mindsdb_storage')
        create_directory(path)

    return path
