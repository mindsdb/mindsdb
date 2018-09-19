"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

import os
import logging

import inspect

def ifEnvElse(env_var, else_value):
    """
    return else_value if env_var is not set in environment variables
    :return:
    """
    return else_value if env_var not in os.environ else os.environ[env_var]


PROXY_SERVER_PORT = ifEnvElse('MINDSDB_PROXY_SERVER_PORT', 3306)
PROXY_SERVER_HOST = ifEnvElse('MINDSDB_PROXY_SERVER_HOST', 'localhost')
PROXY_LOG_CONFIG = {
    'format': ifEnvElse('MINDSDB_PROXY_LOG_FORMAT', '[%(levelname)s] %(message)s'),
    'level': ifEnvElse('MINDSDB_PROXY_LOG_LEVEL', logging.INFO),
    'filename': ifEnvElse('MINDSDB_PROXY_LOG_FILENAME', None)
}

SQLITE_FILE = ifEnvElse('SQLITE_FILE', '/tmp/mindsdb.mdb')
MYSQL_PROXY = ifEnvElse('MYSQL_PROXY', False)
WEBSOCKET_PROXY = ifEnvElse('WEBSOCKET_PROXY', True)
LOGGING_WEBSOCKET_PROXY = ifEnvElse('LOGGING_WEBSOCKET_PROXY', True)


MONGO_SERVER_PORT = ifEnvElse('MONGO_SERVER_PORT', 27017)
MONGO_SERVER_HOST = ifEnvElse('MONGO_SERVER_HOST', 'mongodb://127.0.0.1/mindsdb')
TEST_TRAIN_RATIO = ifEnvElse('TEST_TRAIN_RATIO', 1.0/10.0)


USE_CUDA = ifEnvElse('USE_CUDA', False)
TS_PREDICT_X_PERIODS = ifEnvElse('TS_PREDICT_X_PERIODS', 10)


DEFAULT_MARGIN_OF_ERROR = ifEnvElse('DEFAULT_MARGIN_OF_ERROR', 0.02)
DEFAULT_CONFIDENCE_LEVEL = ifEnvElse('DEFAULT_CONFIDENCE_LEVEL', 0.98)


TEST_OVERWRITE_MODEL = ifEnvElse('TEST_OVERWRITE_MODEL', True) # flip this to true if coding offline
EXEC_LEARN_IN_THREAD = ifEnvElse('EXEC_LEARN_IN_THREAD', False)


SAMPLER_MAX_BATCH_SIZE =  ifEnvElse('SAMPLER_MAX_BATCH_SIZE', 1000)


WEBSOCKET_URL = ifEnvElse('WEBSOCKET_URL', "ws://127.0.0.1:9000")
LOGGING_WEBSOCKET_URL = ifEnvElse('LOGGING_WEBSOCKET_URL', "ws://127.0.0.1:9001")


MINDSDB_STORAGE_PATH = os.path.abspath(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))+'/../storage/')


try:
    from mindsdb.config.personal_config import *
except:
    logging.debug('No personal config (NOTE: you can set personal configs in config/presonal_config.py)')
