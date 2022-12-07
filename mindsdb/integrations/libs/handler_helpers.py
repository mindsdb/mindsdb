import importlib


def get_handler(_type):
    _type = _type.lower()
    # a crutch to fix bug in handler naming convention
    if _type == "files":
        _type = "file"
    handler_folder_name = _type + "_handler"
    try:
        handler_module = importlib.import_module(f'mindsdb.integrations.handlers.{handler_folder_name}')
        return handler_module.Handler
    except Exception as e:
        raise e
