import logging
from logging.config import dictConfig

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
        logging.CRITICAL: logging.Formatter(bold_red + format + reset)
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        return log_fmt.format(record)

def configure_logging():
    logging_config = dict(
        version=1,
        formatters={
            "f": {
                "()": ColorFormatter
            }
        },
        handlers={
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "f",
                "level": logging.DEBUG,
            }
        },
        loggers={
            "": {  # root logger
                "handlers": ["console"],
                "level": logging.WARNING,
            },
            "__main__": {
                "handlers": ["console"],
                "level": logging.DEBUG,
            },
            "mindsdb": {
                "handlers": ["console"],
                "level": logging.DEBUG,
            },
            "alembic": {
                "handlers": ["console"],
                "level": logging.DEBUG,
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
