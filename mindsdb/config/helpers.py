import os
from mindsdb.version import mindsdb_version
import logging

import inspect

def ifEnvElse(env_var, else_value):
    """
    return else_value if env_var is not set in environment variables
    :return:
    """
    return else_value if env_var not in os.environ else os.environ[env_var]

MINDSDB_CONFIG_VARS = {}

def set(var, val, mindsdb_config_vars_pointer = None):
    """
    this is used to set a global variable
    :param var:  the variable you want to set
    :param val:  the value you want to set to the variable
    :param mindsdb_config_vars_pointer: the list of global variables, NOTE: we only use this on config/__init__.py
    :return: None
    """
    global MINDSDB_CONFIG_VARS

    # set the config vars pointer if passed
    if mindsdb_config_vars_pointer is not None:
        MINDSDB_CONFIG_VARS = mindsdb_config_vars_pointer

    MINDSDB_CONFIG_VARS[var] = val

def getMindsDBStoragePath():
    return os.path.abspath('{mindsdb_path}/../mindsdb_storage/{version}'.format(mindsdb_path=getMindsDBPath(),
                                                                         version=mindsdb_version.replace('.', '_')))
def getMindsDBPath():
    return os.path.abspath(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))+'/../')