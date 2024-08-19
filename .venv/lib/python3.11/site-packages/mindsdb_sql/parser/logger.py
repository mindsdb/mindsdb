import logging
import os
import sys
from sly.yacc import SlyLogger


class ParserLogger(SlyLogger):
    def __init__(self, f=sys.stderr, logger=None):
        super().__init__(f)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(os.environ.get('MINDSDB_SQL_LOGLEVEL', 'ERROR'))

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args)
