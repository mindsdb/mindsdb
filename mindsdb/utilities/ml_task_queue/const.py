from enum import Enum


TASKS_STREAM_NAME = b'ml-tasks'
TASKS_STREAM_CONSUMER_GROUP_NAME = 'ml_executors'
TASKS_STREAM_CONSUMER_NAME = 'ml_executor'


class ML_TASK_TYPE(Enum):
    LEARN = b'learn'
    PREDICT = b'predict'
    FINETUNE = b'finetune'


class ML_TASK_STATUS(Enum):
    WAITING = b'waiting'
    PROCESSING = b'processing'
    COMPLETE = b'complete'
    ERROR = b'error'
    TIMEOUT = b'timeout'
