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

from mindsdb.interfaces.storage.db import PREDICTOR_STATUS  # noqa
