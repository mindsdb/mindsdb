import inspect
import os
from pathlib import Path

from mindsdb.__about__ import __version__


def ifEnvElse(env_var, else_value):
    """
    return else_value if env_var is not set in environment variables
    :return:
    """
    return else_value if env_var not in os.environ else os.environ[env_var]



def get_and_create_default_storage_path():
    mindsdb_path = os.path.abspath(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))+'/../../')
    path = os.path.abspath(f'{mindsdb_path}/mindsdb_storage/{__version__.replace('.', '_')}')

    # if it does not exist try to create it
    if not os.path.exists(path):
        try:
            self.log.info(f'{path} does not exist, creating it now')
            path = Path(path)
            path.mkdir(exist_ok=True, parents=True)
        except:
            self.log.error(traceback.format_exc())
            self.log.error(f'MindsDB storage foldler: {path} does not exist and could not be created')


    if not os.access(CONFIG.MINDSDB_STORAGE_PATH, os.W_OK):
