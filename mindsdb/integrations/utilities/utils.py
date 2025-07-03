from typing import Any

import sys


def format_exception_error(exception):
    try:
        exception_type, _exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        error_message = f"{exception_type.__name__}: {exception}, raised at: {filename}#{line_number}"
    except Exception:
        error_message = str(exception)
    return error_message


def dict_to_yaml(d, indent=0):
    yaml_str = ""
    for k, v in d.items():
        yaml_str += " " * indent + str(k) + ": "
        if isinstance(v, dict):
            yaml_str += "\n" + dict_to_yaml(v, indent + 2)
        else:
            yaml_str += str(v) + "\n"
    return yaml_str


# Mocks won't always have 'name' attribute.
def get_class_name(instance: Any, default: str = "unknown"):
    if hasattr(instance.__class__, "name"):
        return instance.__class__.name
    return default
