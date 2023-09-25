from enum import Enum


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
