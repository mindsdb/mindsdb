class TableNotFound(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class InvalidNativeQuery(Exception):
    pass


class TableAlreadyExists(Exception):
    pass


class MissingConnectionParams(Exception):
    pass
