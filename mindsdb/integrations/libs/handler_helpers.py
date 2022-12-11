import importlib
from mindsdb.utilities.log import get_log
logger = get_log(logger_name="main")


def get_handler(_type):
    _type = _type.lower()
    # a crutch to fix bug in handler naming convention
    if _type == "files":
        _type = "file"
    handler_folder_name = _type + "_handler"
    logger.debug("get_handler: handler_folder - %s", handler_folder_name)

    try:
        handler_module = importlib.import_module(f'mindsdb.integrations.handlers.{handler_folder_name}')
        logger.debug("get_handler: handler module - %s", handler_module)
        handler = handler_module.Handler
        if handler is None:
            logger.error("get_handler: import error - %s", handler_module.import_error)
        logger.debug("get_handler: found handler - %s", handler)
        return handler
    except Exception as e:
        raise e
