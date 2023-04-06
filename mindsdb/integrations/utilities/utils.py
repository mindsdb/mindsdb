import sys
from mindsdb_sql.parser.ast import Identifier, Constant, BinaryOperation
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController


def format_exception_error(exception):
    try:
        exception_type, _exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        error_message = f'{exception_type.__name__}: {exception}, raised at: {filename}#{line_number}'
    except Exception:
        error_message = str(exception)
    return error_message
