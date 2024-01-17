

# base exception for unknown error
class UnknownError(Exception):
    # err_code = ERR.ER_UNKNOWN_ERROR
    pass


# base exception for known error
class ExecutorException(Exception):
    pass


class NotSupportedYet(ExecutorException):
    pass


class BadDbError(ExecutorException):
    pass


class BadTableError(ExecutorException):
    pass


class KeyColumnDoesNotExist(ExecutorException):
    pass


class TableNotExistError(ExecutorException):
    pass


class WrongArgumentError(ExecutorException):
    pass


class LogicError(ExecutorException):
    pass
