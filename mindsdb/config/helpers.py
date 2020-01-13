import inspect
import os
from pathlib import Path
import traceback

from mindsdb.__about__ import __version__


def if_env_else(env_var, else_value):
    """
    return else_value if env_var is not set in environment variables
    :return:
    """
    return else_value if env_var not in os.environ else os.environ[env_var]


def create_directory(path):
    print(f'{path} does not exist, creating it now')
    path = Path(path)
    path.mkdir(mode=0o777, exist_ok=True, parents=True)


def get_and_create_default_storage_path():
    print( os.path.abspath(inspect.getfile(inspect.currentframe())) )
    mindsdb_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), '/../../'))
    version_path = __version__.replace('.', '_')
    path = os.path.abspath(f'{mindsdb_path}/mindsdb_storage/{version_path}')


    try:
        create_directory(path)
        correct_permissions = os.access(path, os.W_OK)
    except:
        correct_permissions = False

    if not correct_permissions:
        home = os.path.expanduser('~')
        path = os.path.join(home, '.mindsdb_storage')
        try:
            create_directory(path)
        except:
            print(f'MindsDB storage directory: {path} does not exist and could not be created')

    return path
