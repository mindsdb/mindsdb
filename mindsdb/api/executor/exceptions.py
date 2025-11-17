from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import ERR


# base exception for unknown error
class UnknownError(Exception):
    mysql_error_code = ERR.ER_UNKNOWN_ERROR
    is_expected = False


# base exception for known error
class ExecutorException(Exception):
    mysql_error_code = ERR.ER_UNKNOWN_ERROR
    is_expected = False


class NotSupportedYet(ExecutorException):
    mysql_error_code = ERR.ER_NOT_SUPPORTED_YET
    is_expected = True


class BadDbError(ExecutorException):
    mysql_error_code = ERR.ER_BAD_DB_ERROR
    is_expected = True


class BadTableError(ExecutorException):
    mysql_error_code = ERR.ER_BAD_DB_ERROR
    is_expected = True


class KeyColumnDoesNotExist(ExecutorException):
    mysql_error_code = ERR.ER_KEY_COLUMN_DOES_NOT_EXIST
    is_expected = True


class TableNotExistError(ExecutorException):
    mysql_error_code = ERR.ER_TABLE_EXISTS_ERROR
    is_expected = True


class WrongArgumentError(ExecutorException):
    mysql_error_code = ERR.ER_WRONG_ARGUMENTS
    is_expected = True


class LogicError(ExecutorException):
    mysql_error_code = ERR.ER_WRONG_USAGE
    is_expected = True


class SqlSyntaxError(ExecutorException):
    err_code = ERR.ER_SYNTAX_ERROR
    is_expected = True


class WrongCharsetError(ExecutorException):
    err_code = ERR.ER_UNKNOWN_CHARACTER_SET
    is_expected = True
