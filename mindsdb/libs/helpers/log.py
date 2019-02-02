import pprint
import logging
import time
from logging import handlers
import colorlog
from inspect import getframeinfo, stack
import socketio

global internal_logger
global id
global sio



def gen_char(n, ch):
    return ''.join([ch for i in range(n)])


def initialize(uuid, level, log_url):
    global id
    global internal_logger
    global sio

    id = uuid
    internal_logger = logging.getLogger('mindsdb-logger-{}'.format(id))

    sio = socketio.Client()

    @sio.on('connect')
    def on_connect():
        print('connection established')

    @sio.on('send_url')
    def on_call(payload):
        internal_logger.info('\n\n{eq1}\n{eq2}   You can view your logs at: {url}   {eq2}\n{eq1}\n\n'.format(eq1=gen_char(104, "="), eq2=gen_char(2, "|"), url=payload['url']))

    @sio.on('disconnect')
    def on_disconnect():
        print('disconnected from server')

    sio.connect(log_url)

    internal_logger.handlers = []
    internal_logger.propagate = False

    internal_logger
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    internal_logger.addHandler(stream_handler)

    internal_logger.setLevel(level)

def debug(message):
    global sio

    message = pprint.pformat(str(message))
    caller = getframeinfo(stack()[1][0])
    internal_logger.debug("%s:%d - %s" % (caller.filename.split('mindsdb/')[-1], caller.lineno, message))

def info(message):
    global sio

    message = pprint.pformat(str(message))
    caller = getframeinfo(stack()[1][0])
    sio.emit('call',{'message':str(message),'uuid':id})
    internal_logger.info("%s:%d - %s" % (caller.filename.split('mindsdb/')[-1], caller.lineno, message))

def warning(message):
    global sio

    message = pprint.pformat(str(message))
    sio.emit('call',{'message':str(message),'uuid':id})
    caller = getframeinfo(stack()[1][0])
    internal_logger.warning("%s:%d - %s" % (caller.filename.split('mindsdb/')[-1], caller.lineno, message))

def error(message):
    global sio

    message = pprint.pformat(str(message))
    sio.emit('call',{'message':str(message),'uuid':id})

    caller = getframeinfo(stack()[1][0])
    internal_logger.error("%s:%d - %s" % (caller.filename.split('mindsdb/')[-1], caller.lineno, message))
    sio.disconnect()

def infoChart(message,type,uid=None):
    pass


def __exit__(self, exc_type, exc_value, traceback):
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n!!!!!!!!!!!!!!!!!!!\n\n\n\n\n")
    exit()
