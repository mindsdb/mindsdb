"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *******************************************************
"""


import logging
from .helpers import *
import  mindsdb.libs.constants.mindsdb as CONST

class Config:
    # These are the paths for storing data regarding mindsdb models and model info
    MINDSDB_STORAGE_PATH = ifEnvElse('MINDSDB_STORAGE_PATH', getMindsDBStoragePath())
    
    LOCALSTORE_PATH_TEMPLATE = '{storage_path}/local_jsondb_store'
    LOCALSTORE_PATH = ifEnvElse('LOCALSTORE_PATH', LOCALSTORE_PATH_TEMPLATE.format(storage_path=MINDSDB_STORAGE_PATH))

    # You can choose to store info about models in MongoDB or not By default it uses a local object store
    STORE_INFO_IN_MONGODB = ifEnvElse('STORE_INFO_IN_MONGODB', False)

    # If STORE_INFO_IN_MONGODB == True
    MONGO_SERVER_HOST = ifEnvElse('MONGO_SERVER_HOST', 'mongodb://127.0.0.1/mindsdb')

    # What percentage of data do we want to keep as test, and what as train default 10% is test
    TEST_TRAIN_RATIO = ifEnvElse('TEST_TRAIN_RATIO', 0.1)

    # If you want to use CUDA
    USE_CUDA = ifEnvElse('CONFIG.USE_CUDA', False)
    if USE_CUDA == 'True' or USE_CUDA == 'true' or USE_CUDA == 1 or USE_CUDA == '1':
        USE_CUDA = True

    # THESE ARE VALUES TO ESTIMATE THE SAMPLE SIZE TO GATHER STATISTICAL DATA ABOUT THE DATASET
    DEFAULT_MARGIN_OF_ERROR = ifEnvElse('DEFAULT_MARGIN_OF_ERROR', 0.00)
    DEFAULT_CONFIDENCE_LEVEL = ifEnvElse('DEFAULT_CONFIDENCE_LEVEL', 0.98)

    # IF YOU CAN TO MOVE THE TRAINING OPERATION TO A DIFFERENT EXECUTION THREAD (DEFAULT True)
    EXEC_LEARN_IN_THREAD = ifEnvElse('EXEC_LEARN_IN_THREAD', False)

    # How big do we want each batch size at training
    SAMPLER_MAX_BATCH_SIZE =  ifEnvElse('SAMPLER_MAX_BATCH_SIZE', 1000)
    # We can take in numeric columns as text when the number of unique values in the column is less than this flag
    ASSUME_NUMERIC_AS_TEXT_WHEN_UNIQUES_IS_LESS_THAN =  ifEnvElse('ASSUME_NUMERIC_AS_TEXT_WHEN_UNIQUES_IS_LESS_THAN', 200)

    # MindsDB has various Proxies that you can plug into
    MINDSDB_SERVER_URL = ifEnvElse('MINDSDB_SERVER_URL', 'http://localhost:35261')

    # LOG Config settings
    DEFAULT_LOG_LEVEL = ifEnvElse('DEFAULT_LOG_LEVEL', CONST.INFO_LOG_LEVEL)

    # If logs should be streamed to a server
    SEND_LOGS = ifEnvElse('SEND_LOGS', False)

    # Debug config variable (Do not change this unless you are developing phases in mindsdb)
    DEBUG_BREAK_POINT = ifEnvElse('DEBUG_BREAK_POINT', CONST.PHASE_END)


CONFIG = Config()
