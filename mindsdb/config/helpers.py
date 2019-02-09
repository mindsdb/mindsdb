import os
from mindsdb.version import mindsdb_version
import inspect

def ifEnvElse(env_var, else_value):
    """
    return else_value if env_var is not set in environment variables
    :return:
    """
    return else_value if env_var not in os.environ else os.environ[env_var]



def getMindsDBStoragePath():
    return os.path.abspath('{mindsdb_path}/mindsdb_storage/{version}'.format(mindsdb_path=getMindsDBPath(),
                                                                         version=mindsdb_version.replace('.', '_')))
def getMindsDBPath():
    return os.path.abspath(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))+'/../../')