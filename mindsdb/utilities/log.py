import os
import logging

from mindsdb.utilities.config import Config
from functools import partial

config = Config().get_all()

log = logging.getLogger('mindsdb')
log.propagate = False
log.setLevel(min(
    getattr(logging, config['log']['level']['console']),
    getattr(logging, config['log']['level']['file'])
))

formatter = logging.Formatter('%(levelname)s: - %(asctime)s - %(name)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setLevel(config['log']['level'].get('console', logging.INFO))
console_handler.setFormatter(formatter)

log.handlers.clear()
log.addHandler(console_handler)
log.info(f"Root handler set to {console_handler.level}.")
log.info(f"log level {log.level}")
log.info(f"Number of handlers: {len(log.handlers)}")
log.info(f"")

log.error = partial(log.error, exc_info=True)

logger = log

# activate telemetry
telemtry_enabled = os.getenv('CHECK_FOR_UPDATES', '1').lower() not in ['0', 'false', 'False']
if telemtry_enabled:
    try:
        import sentry_sdk

        sentry_sdk.init(
            "https://29e64dbdf325404ebf95473d5f4a54d3@o404567.ingest.sentry.io/5633566",
            traces_sample_rate=0  # Set to `1` to experiment with performance metrics
        )
    except (ImportError, ModuleNotFoundError) as e:
        raise Exception(f"to use telemetry please install 'pip install mindsdb[telemetry]': {e}")


def initialize_log(config=None, logger_name='main', wrap_print=None):
    """
    This function sets the global logger used by get_log to the thread specific logger.
    """
    global logger
    logger = logging.getLogger(f'mindsdb.{logger_name}')


def get_log(logger_name=None):
    """
    Creates child loggers from the mindsdb logger
    """

    if logger_name is None:
        return logger
    return logger.getChild(logger_name)
