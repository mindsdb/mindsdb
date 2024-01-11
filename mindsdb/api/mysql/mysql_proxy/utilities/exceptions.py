from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import ERR


# base exception for known error
class SqlApiException(Exception):
    err_code = ERR.ER_SYNTAX_ERROR


class ErSqlSyntaxError(SqlApiException):
    err_code = ERR.ER_SYNTAX_ERROR


class ErWrongCharset(SqlApiException):
    err_code = ERR.ER_UNKNOWN_CHARACTER_SET
