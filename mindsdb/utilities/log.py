import logging
from logging.config import dictConfig

from mindsdb.utilities.config import config as app_config


logging_initialized = False


class ColorFormatter(logging.Formatter):
    green = "\x1b[32;20m"
    default = "\x1b[39;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s %(processName)15s %(levelname)-8s %(name)s: %(message)s"

    FORMATS = {
        logging.DEBUG: logging.Formatter(green + format + reset),
        logging.INFO: logging.Formatter(default + format + reset),
        logging.WARNING: logging.Formatter(yellow + format + reset),
        logging.ERROR: logging.Formatter(red + format + reset),
        logging.CRITICAL: logging.Formatter(bold_red + format + reset),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        return log_fmt.format(record)


def get_console_handler_config_level() -> int:
    console_handler_config = app_config['logging']['handlers']['console']
    return getattr(logging, console_handler_config["level"])


def get_file_handler_config_level() -> int:
    file_handler_config = app_config['logging']['handlers']['file']
    return getattr(logging, file_handler_config["level"])


def get_mindsdb_log_level() -> int:
    console_handler_config_level = get_console_handler_config_level()
    file_handler_config_level = get_file_handler_config_level()

    return min(console_handler_config_level, file_handler_config_level)


def configure_logging():
    handlers_config = {}
    console_handler_config = app_config['logging']['handlers']['console']
    console_handler_config_level = getattr(logging, console_handler_config["level"])
    if console_handler_config['enabled'] is True:
        handlers_config['console'] = {
            "class": "logging.StreamHandler",
            "formatter": "f",
            "level": console_handler_config_level
        }

    file_handler_config = app_config['logging']['handlers']['file']
    file_handler_config_level = getattr(logging, file_handler_config["level"])
    if file_handler_config['enabled'] is True:
        handlers_config['file'] = {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "file",
            "level": file_handler_config_level,
            "filename": app_config.paths["log"] / file_handler_config["filename"],
            "maxBytes": file_handler_config["maxBytes"],  # 0.5 Mb
            "backupCount": file_handler_config["backupCount"]
        }

    mindsdb_log_level = get_mindsdb_log_level()

    logging_config = dict(
        version=1,
        formatters={
            "f": {"()": ColorFormatter},
            "file": {
                "format": "%(asctime)s %(processName)15s %(levelname)-8s %(name)s: %(message)s"
            }
        },
        handlers=handlers_config,
        loggers={
            "": {  # root logger
                "handlers": list(handlers_config.keys()),
                "level": mindsdb_log_level,
            },
            "__main__": {
                "level": mindsdb_log_level,
            },
            "mindsdb": {
                "level": mindsdb_log_level,
            },
            "alembic": {
                "level": mindsdb_log_level,
            },
        },
    )

    dictConfig(logging_config)


# I would prefer to leave code to use logging.getLogger(), but there are a lot of complicated situations
# in MindsDB with processes being spawned that require logging to be configured again in a lot of cases.
# Using a custom logger-getter like this lets us do that logic here, once.
def getLogger(name=None):
    """
    Get a new logger, configuring logging first if it hasn't been done yet.
    """
    global logging_initialized
    if not logging_initialized:
        configure_logging()
        logging_initialized = True

    return logging.getLogger(name)
