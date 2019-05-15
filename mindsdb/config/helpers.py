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



def getMindsDBStoragePath():
    return os.path.abspath('{mindsdb_path}/mindsdb_storage/{version}'.format(mindsdb_path=getMindsDBPath(),
                                                                         version=__version__.replace('.', '_')))
def getMindsDBPath():
    return os.path.abspath(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))+'/../../')
