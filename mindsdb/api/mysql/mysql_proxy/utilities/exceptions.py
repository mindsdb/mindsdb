from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import ERR

class SqlApiException(Exception):
    err_code = ERR.ER_SYNTAX_ERROR



class ErBadDbError(SqlApiException):
    err_code = ERR.ER_BAD_DB_ERROR

class ErBadTableError(SqlApiException):
    err_code = ERR.ER_BAD_DB_ERROR

class ErKeyColumnDoesNotExist(SqlApiException):
    err_code = ERR.ER_KEY_COLUMN_DOES_NOT_EXIST

class ErTableExistError(SqlApiException):
    err_code = ERR.ER_TABLE_EXISTS_ERROR

class ErDubFieldName(SqlApiException):
    err_code = ERR.ER_DUP_FIELDNAME

class ErDbDropDelete(SqlApiException):
    err_code = ERR.ER_DB_DROP_DELETE

class ErNonInsertableTable(SqlApiException):
    err_code = ERR.ER_NON_INSERTABLE_TABLE

class ErNotSupportedYet(SqlApiException):
    err_code = ERR.ER_NOT_SUPPORTED_YET
