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
global send


def gen_chars(length, character):
    '''
    # Generates a string consisting of `length` consiting of repeating `character`

    :param length: length of the string
    :param chracter: character to use
    '''

    return ''.join([character for i in range(length)])


def initialize(level, log_url, send_logs, uuid):
    '''
    # Initialize the log module, should only be called once at the begging of the program

    :param level: What logs to display
    :param log_url: What urls to send logs to
    :param send_logs: Whether or not to send logs to the remote Mindsdb server
    :param uuid: The unique id for this MindsDB instance or training/prediction session
    '''

    global id
    global internal_logger
    global sio
    global send

    id = uuid
    internal_logger = logging.getLogger('mindsdb-logger-{}'.format(id))

    send = send_logs
    if send:
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


def log_message(message, func):
    '''
    # Internal function used for logging, adds the id and caller to the log and prettifies the message

    :param message: message that the logger shoud log
    :param chracter: logger function to use (example: 'info' or 'error')
    '''
    global send
    global sio

    caller = getframeinfo(stack()[2][0])
    message = pprint.pformat(str(message))
    if send and func != 'debug':
        sio.emit('call',{'message':str(message),'uuid':id})

    call = getattr(internal_logger, func)
    call("%s:%d - %s" % (caller.filename.split('mindsdb/')[-1], caller.lineno, message))

def debug(message):
    log_message(message, 'debug')

def info(message):
    log_message(message, 'info')

def warning(message):
    log_message(message, 'warning')

def error(message):
    log_message(message, 'error')
    if send:
        sio.disconnect()

def infoChart(message,type,uid=None):
    pass


def __exit__(self, exc_type, exc_value, traceback):
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n!!!!!!!!!!!!!!!!!!!\n\n\n\n\n")
    exit()
