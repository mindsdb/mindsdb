import os
import sys
import logging
import traceback

from mindsdb.utilities.config import Config
from functools import partial


class LoggerWrapper(object):
    def __init__(self, writer_arr, default_writer_pos):
        self._writer_arr = writer_arr
        self.default_writer_pos = default_writer_pos

    def write(self, message):
        if len(message.strip(' \n')) == 0:
            return
        if 'DEBUG:' in message:
            self._writer_arr[0](message)
        elif 'INFO:' in message:
            self._writer_arr[1](message)
        elif 'WARNING:' in message:
            self._writer_arr[2](message)
        elif 'ERROR:' in message:
            self._writer_arr[3](message)
        else:
            self._writer_arr[self.default_writer_pos](message)

    def flush(self):
        pass

    def isatty(self):
        return True  # assumes terminal attachment

    def fileno(self):
        return 1  # stdout

# class DbHandler(logging.Handler):
#     def __init__(self):
#         logging.Handler.__init__(self)
#         self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
#
#     def emit(self, record):
#         self.format(record)
#         if (
#             len(record.message.strip(' \n')) == 0
#             or (record.threadName == 'ray_print_logs' and 'mindsdb-logger' not in record.message)
#         ):
#             return
#
#         log_type = record.levelname
#         source = f'file: {record.pathname} - line: {record.lineno}'
#         payload = record.msg
#
#         if telemtry_enabled:
#             pass
#             # @TODO: Enable once we are sure no sensitive info is being outputed in the logs
#             # if log_type in ['INFO']:
#             #    add_breadcrumb(
#             #        category='auth',
#             #        message=str(payload),
#             #        level='info',
#             #    )
#             # Might be too much traffic if we send this for users with slow networks
#             # if log_type in ['DEBUG']:
#             #    add_breadcrumb(
#             #        category='auth',
#             #        message=str(payload),
#             #        level='debug',
#             #    )
#
#         if log_type in ['ERROR', 'WARNING']:
#             trace = str(traceback.format_stack(limit=20))
#             trac_log = Log(log_type='traceback', source=source, payload=trace, company_id=self.company_id)
#             session.add(trac_log)
#             session.commit()
#
#             if telemtry_enabled:
#                 add_breadcrumb(
#                     category='stack_trace',
#                     message=trace,
#                     level='info',
#                 )
#                 if log_type in ['ERROR']:
#                     capture_message(str(payload))
#                 if log_type in ['WARNING']:
#                     capture_message(str(payload))
#
#         log = Log(log_type=str(log_type), source=source, payload=str(payload), company_id=self.company_id)
#         session.add(log)
#         session.commit()

# default logger
logger = logging.getLogger('dummy')


def initialize_log(config=None, logger_name='main', wrap_print=False):
    global logger
    if config is None:
        config = Config().get_all()

    telemtry_enabled = os.getenv('CHECK_FOR_UPDATES', '1').lower() not in ['0', 'false', 'False']

    if telemtry_enabled:
        import sentry_sdk
        from sentry_sdk import capture_message, add_breadcrumb
        sentry_sdk.init(
            "https://29e64dbdf325404ebf95473d5f4a54d3@o404567.ingest.sentry.io/5633566",
            traces_sample_rate=0  # Set to `1` to experiment with performance metrics
        )

    ''' Create new logger
    :param config: object, app config
    :param logger_name: str, name of logger
    :param wrap_print: bool, if true, then print() calls will be wrapped by log.debug() function.
    '''
    log = logging.getLogger(f'mindsdb.{logger_name}')
    log.propagate = False
    log.setLevel(min(
        getattr(logging, config['log']['level']['console']),
        getattr(logging, config['log']['level']['file'])
    ))

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(config['log']['level'].get('console', logging.INFO))
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    # db_handler = DbHandler()
    # db_handler.setLevel(config['log']['level'].get('db', logging.WARNING))
    # db_handler.setFormatter(formatter)
    # log.addHandler(db_handler)

    if wrap_print:
        sys.stdout = LoggerWrapper([log.debug, log.info, log.warning, log.error], 1)
        sys.stderr = LoggerWrapper([log.debug, log.info, log.warning, log.error], 3)

    log.error = partial(log.error, exc_info=True)
    logger = log


def get_log(logger_name=None):
    if logger_name is None:
        return logging.getLogger('mindsdb')
    return logging.getLogger(f'mindsdb.{logger_name}')

