import os
import sys
import logging


class LoggerWrapper(object):
    def __init__(self, writer):
        self._writer = writer
        self._msg = ''

    def write(self, message):
        self._msg = self._msg + message
        while '\n' in self._msg:
            pos = self._msg.find('\n')
            self._writer(self._msg[:pos])
            self._msg = self._msg[pos + 1:]

    def flush(self):
        if self._msg != '':
            self._writer(self._msg)
            self._msg = ''


def initialize_log(config, logger_name='main', wrap_print=False):
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

    ch = logging.StreamHandler()
    ch.setLevel(config['log']['level']['console'])       # that level will be in console
    log.addHandler(ch)

    log_path = os.path.join(config.paths['log'], logger_name)
    if not os.path.isdir(log_path):
        os.mkdir(log_path)

    fh = logging.handlers.RotatingFileHandler(
        os.path.join(log_path, 'log.txt'),
        mode='a',
        encoding='utf-8',
        maxBytes=100 * 1024,
        backupCount=3
    )
    fh.setLevel(config['log']['level']['file'])
    fh.setFormatter(formatter)
    log.addHandler(fh)

    if wrap_print:
        sys.stdout = LoggerWrapper(log.debug)


log = logging.getLogger('mindsdb')
