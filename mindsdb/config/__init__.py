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

    # What percentage of data do we want to keep as test, and what as train default 10% is test
    TEST_TRAIN_RATIO = ifEnvElse('TEST_TRAIN_RATIO', 0.1)

    # THESE ARE VALUES TO ESTIMATE THE SAMPLE SIZE TO GATHER STATISTICAL DATA ABOUT THE DATASET
    DEFAULT_MARGIN_OF_ERROR = ifEnvElse('DEFAULT_MARGIN_OF_ERROR', 0.00)
    DEFAULT_CONFIDENCE_LEVEL = ifEnvElse('DEFAULT_CONFIDENCE_LEVEL', 0.98)

    # IF YOU CAN TO MOVE THE TRAINING OPERATION TO A DIFFERENT EXECUTION THREAD (DEFAULT True)
    EXEC_LEARN_IN_THREAD = ifEnvElse('EXEC_LEARN_IN_THREAD', False)

    # LOG Config settings
    DEFAULT_LOG_LEVEL = ifEnvElse('DEFAULT_LOG_LEVEL', CONST.INFO_LOG_LEVEL)

    # If logs should be streamed to a server
    SEND_LOGS = ifEnvElse('SEND_LOGS', False)

CONFIG = Config()
