from enum import Enum


TASKS_STREAM_NAME = b'ml-tasks'
TASKS_STREAM_CONSUMER_GROUP_NAME = 'ml_executors'
TASKS_STREAM_CONSUMER_NAME = 'ml_executor'


class ML_TASK_TYPE(Enum):
    LEARN = b'learn'
    PREDICT = b'predict'
    FINETUNE = b'finetune'
    DESCRIBE = b'describe'
    CREATE_VALIDATION = b'create_validation'
    CREATE_ENGINE = b'create_engine'
    UPDATE_ENGINE = b'update_engine'
    UPDATE = b'update'
    FUNC_CALL = b'func_call'


class ML_TASK_STATUS(Enum):
    WAITING = b'waiting'
    PROCESSING = b'processing'
    COMPLETE = b'complete'
    ERROR = b'error'
    TIMEOUT = b'timeout'
