"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *******************************************************
"""


import logging
from .helpers import *
import mindsdb.libs.constants.mindsdb as CONST

class Config:
    # These are the paths for storing data regarding mindsdb models and model info
    MINDSDB_STORAGE_PATH = if_env_else('MINDSDB_STORAGE_PATH', get_and_create_default_storage_path())

    # What percentage of data do we want to keep as test, and what as train default 10% is test
    TEST_TRAIN_RATIO = if_env_else('TEST_TRAIN_RATIO', 0.1)

    # IF YOU CAN TO MOVE THE TRAINING OPERATION TO A DIFFERENT EXECUTION THREAD (DEFAULT True)
    EXEC_LEARN_IN_THREAD = if_env_else('EXEC_LEARN_IN_THREAD', False)

    # LOG Config settings
    DEFAULT_LOG_LEVEL = if_env_else('DEFAULT_LOG_LEVEL', CONST.DEBUG_LOG_LEVEL)

    CHECK_FOR_UPDATES = if_env_else('CHECK_FOR_UPDATES', True)
    IS_CI_TEST = if_env_else('IS_CI_TEST', False)

    # Default options for unning on sagemaker
    SAGEMAKER = if_env_else('SAGEMAKER', False)


CONFIG = Config()
