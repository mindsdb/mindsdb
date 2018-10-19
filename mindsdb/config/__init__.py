"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *******************************************************
"""


from .helpers import *


# These are the paths for storing data regarding mindsdb models and model info
MINDSDB_STORAGE_PATH = ifEnvElse('MINDSDB_STORAGE_PATH', getMindsDBStoragePath())

SQLITE_FILE = ifEnvElse('SQLITE_FILE', '{storage_path}/mindsdb.mdb'.format(storage_path=MINDSDB_STORAGE_PATH))
LOCALSTORE_PATH = ifEnvElse('LOCALSTORE_PATH', '{storage_path}/local_jsondb_store'.format(storage_path=MINDSDB_STORAGE_PATH))

# You can choose to store info about models in MongoDB or not By default it uses a local object store
STORE_INFO_IN_MONGODB = ifEnvElse('STORE_INFO_IN_MONGODB', False)

# If STORE_INFO_IN_MONGODB == True
MONGO_SERVER_HOST = ifEnvElse('MONGO_SERVER_HOST', 'mongodb://127.0.0.1/mindsdb')

# What percentage of data do we want to keep as test, and what as train default 10% is test
TEST_TRAIN_RATIO = ifEnvElse('TEST_TRAIN_RATIO', 0.1)

# If you want to use CUDA
USE_CUDA = ifEnvElse('USE_CUDA', False)

# THESE ARE VALUES TO ESTIMATE THE SAMPLE SIZE TO GATHER STATISTICAL DATA ABOUT THE DATASET
DEFAULT_MARGIN_OF_ERROR = ifEnvElse('DEFAULT_MARGIN_OF_ERROR', 0.02)
DEFAULT_CONFIDENCE_LEVEL = ifEnvElse('DEFAULT_CONFIDENCE_LEVEL', 0.98)

# IF YOU CAN TO MOVE THE TRAINING OPERATION TO A DIFFERENT EXECUTION THREAD (DEFAULT True)
EXEC_LEARN_IN_THREAD = ifEnvElse('EXEC_LEARN_IN_THREAD', False)

# How big do we want each batch size at training
SAMPLER_MAX_BATCH_SIZE =  ifEnvElse('SAMPLER_MAX_BATCH_SIZE', 1000)


# MindsDB has various Proxies that you can plug into
MYSQL_PROXY = ifEnvElse('MYSQL_PROXY', False)

WEBSOCKET_PROXY = ifEnvElse('WEBSOCKET_PROXY', False)
WEBSOCKET_URL = ifEnvElse('WEBSOCKET_URL', "ws://127.0.0.1:9000")

LOGGING_WEBSOCKET_PROXY = ifEnvElse('LOGGING_WEBSOCKET_PROXY', True)
LOGGING_WEBSOCKET_URL = ifEnvElse('LOGGING_WEBSOCKET_URL', "ws://127.0.0.1:9001")

PROXY_SERVER_PORT = ifEnvElse('MINDSDB_PROXY_SERVER_PORT', 3306)
PROXY_SERVER_HOST = ifEnvElse('MINDSDB_PROXY_SERVER_HOST', 'localhost')

# LOG Config settings
PROXY_LOG_CONFIG = {
    'format': ifEnvElse('MINDSDB_PROXY_LOG_FORMAT', '[%(levelname)s] %(message)s'),
    'level': ifEnvElse('MINDSDB_PROXY_LOG_LEVEL', logging.WARNING),
    'filename': ifEnvElse('MINDSDB_PROXY_LOG_FILENAME', None)
}






