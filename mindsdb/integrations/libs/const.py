class HANDLER_TYPE:
    __slots__ = ()
    DATA = 'data'
    ML = 'ml'


HANDLER_TYPE = HANDLER_TYPE()


class HANDLER_CONNECTION_ARG_TYPE:
    __slots__ = ()
    STR = 'str'
    INT = 'int'
    BOOL = 'bool'
    URL = 'url'
    PATH = 'path'


HANDLER_CONNECTION_ARG_TYPE = HANDLER_CONNECTION_ARG_TYPE()


class PREDICTOR_STATUS:
    __slots__ = ()
    COMPLETE = 'complete'
    TRAINING = 'training'
    GENERATING = 'generating'
    ERROR = 'error'
    VALIDATION = 'validation'
    DELETED = 'deleted'


PREDICTOR_STATUS = PREDICTOR_STATUS()
