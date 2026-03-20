class HANDLER_TYPE:
    __slots__ = ()
    DATA = "data"
    ML = "ml"


HANDLER_TYPE = HANDLER_TYPE()


class HANDLER_CONNECTION_ARG_TYPE:
    __slots__ = ()
    STR = "str"
    INT = "int"
    BOOL = "bool"
    URL = "url"
    PATH = "path"
    DICT = "dict"
    PWD = "pwd"


HANDLER_CONNECTION_ARG_TYPE = HANDLER_CONNECTION_ARG_TYPE()


class HANDLER_SUPPORT_LEVEL:
    __slots__ = ()
    MINDSDB = "mindsdb"
    COMMUNITY = "community"  # default


HANDLER_SUPPORT_LEVEL = HANDLER_SUPPORT_LEVEL()


from mindsdb.interfaces.storage.db import PREDICTOR_STATUS  # noqa
