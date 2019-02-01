import pprint
import logging
import time
from logging import handlers
from inspect import getframeinfo, stack

global internal_logger

def initialize(level, send_method=None, host='0.0.0.0', por=6666):
    global internal_logger
    internal_logger = logging.getLogger('mindsdb')

    internal_logger.setLevel(level)
    stream_handler = logging.StreamHandler()
    internal_logger.addHandler(stream_handler)

    # @TODO: Add secure tcp transfer, https and wss and possibly some popular library handlers such as logstash
    send_handler = None
    if send_method == 'http':
        send_handler = HTTPHandler(host, port)
    elif send_method == 'ws':
        send_handler = handlers.SocketHandler(host, port)

    if send_handler is not None:
        internal_logger.addHandler(send_handler)

    internal_logger.setLevel(level)

def debug(message):
    message = pprint.pformat(str(message))
    caller = getframeinfo(stack()[1][0])
    internal_logger.debug("%s:%d - %s" % (caller.filename, caller.lineno, message))

def info(message):
    message = pprint.pformat(str(message))
    caller = getframeinfo(stack()[1][0])
    internal_logger.info("%s:%d - %s" % (caller.filename, caller.lineno, message))

def warning(message):
    message = pprint.pformat(str(message))
    caller = getframeinfo(stack()[1][0])
    internal_logger.warning("%s:%d - %s" % (caller.filename, caller.lineno, message))

def error(message):
    message = pprint.pformat(str(message))
    caller = getframeinfo(stack()[1][0])
    internal_logger.error("%s:%d - %s" % (caller.filename, caller.lineno, message))

def infoChart(message,type,uid=None):
    pass
