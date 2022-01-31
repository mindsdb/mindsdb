from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import ERR

class SqlApiException(Exception):
    err_code = ERR.ER_SYNTAX_ERROR



class ErBadDbError(SqlApiException):
    err_code = ERR.ER_BAD_DB_ERROR
