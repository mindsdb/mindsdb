import pprint
import logging
import time
from logging import handlers
import colorlog
from inspect import getframeinfo, stack
import socketio

def gen_chars(length, character):
    '''
    # Generates a string consisting of `length` consiting of repeating `character`

    :param length: length of the string
    :param chracter: character to use
    '''

    return ''.join([character for i in range(length)])


class MindsdbLogger():
    internal_logger = None
    id = None
    sio = None
    send = None

    def initialize(self, level, log_url, send_logs, uuid):
        '''
        # Initialize the log module, should only be called once at the begging of the program

        :param level: What logs to display
        :param log_url: What urls to send logs to
        :param send_logs: Whether or not to send logs to the remote Mindsdb server
        :param uuid: The unique id for this MindsDB instance or training/prediction session
        '''

        self.id = uuid
        self.internal_logger = logging.getLogger('mindsdb-logger-{}'.format(self.id))

        self.send = send_logs
        if self.send:
            self.sio = socketio.Client()

            @sio.on('connect')
            def on_connect():
                print('connection established')

            @sio.on('send_url')
            def on_call(payload):
                self.info('\n\n{eq1}\n{eq2}   You can view your logs at: {url}   {eq2}\n{eq1}\n\n'.format(eq1=gen_char(104, "="), eq2=gen_char(2, "|"), url=payload['url']))

            @sio.on('disconnect')
            def on_disconnect():
                self.warning('disconnected from server')

            self.sio.connect(log_url)

        self.internal_logger.handlers = []
        self.internal_logger.propagate = False

        self.internal_logger
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
        self.internal_logger.addHandler(stream_handler)

        self.internal_logger.setLevel(level)


    def log_message(self, message, func):
        '''
        # Internal function used for logging, adds the id and caller to the log and prettifies the message

        :param message: message that the logger shoud log
        :param chracter: logger function to use (example: 'info' or 'error')
        '''
        caller = getframeinfo(stack()[2][0])
        message = pprint.pformat(str(message))
        if self.send and func != 'debug':
            self.sio.emit('call',{'message':str(message),'uuid':self.id})

        call = getattr(self.internal_logger, func)
        call("%s:%d - %s" % (caller.filename.split('mindsdb/')[-1], caller.lineno, message))

    def debug(self, message):
        self.log_message(message, 'debug')

    def info(self, message):
        self.log_message(message, 'info')

    def warning(self, message):
        self.log_message(message, 'warning')

    def error(self, message):
        self.log_message(message, 'error')
        if self.send:
            self.sio.disconnect()

    def infoChart(self, message,type,uid=None):
        pass
