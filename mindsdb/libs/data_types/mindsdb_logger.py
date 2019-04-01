import pprint
import logging
import colorlog
import socketio
import uuid

from mindsdb import CONFIG
from mindsdb.libs.helpers.text_helpers import gen_chars
from inspect import getframeinfo, stack


class MindsdbLogger():
    internal_logger = None
    id = None
    sio = None
    send = None

    def __init__(self, log_level, log_url, send_logs, uuid):
        '''
        # Initialize the log module, should only be called once at the begging of the program

        :param log_level: What logs to display
        :param log_url: What urls to send logs to
        :param send_logs: Whether or not to send logs to the remote Mindsdb server
        :param uuid: The unique id for this MindsDB instance or training/prediction session
        '''

        self.id = uuid
        self.internal_logger = logging.getLogger('mindsdb-logger-{}'.format(self.id))

        self.send = send_logs



        if self.send:
            sio = socketio.Client()

            @sio.on('connect')
            def on_connect():
                print('connection established')

            @sio.on('send_url')
            def on_call(payload):
                self.info('\n\n{eq1}\n{eq2}   You can view your logs at: {url}   {eq2}\n{eq1}\n\n'.format(eq1=gen_chars(104, "="), eq2=gen_chars(2, "|"), url=payload['url']))

            @sio.on('disconnect')
            def on_disconnect():
                self.warning('disconnected from server')

            self.sio = sio
            self.sio.connect(log_url)

        self.internal_logger.handlers = []
        self.internal_logger.propagate = False

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
        self.internal_logger.addHandler(stream_handler)

        self.internal_logger.setLevel(log_level)


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

    def infoChart(self, message, type, uid=None):
        """
        It will add the specific markdown plus tags or format it for stdout logs

        :param message: its the chart payload
        :param type: the type of chart
        :param uid: the unique id of the chart so markdown can update properly

        :return: None
        """

        if uid is None:
            uid = str(uuid.uuid1())
        if self.send:
            self.info('<chart type="{type}" uid={uid}>'.format(type=type, uid=uid))
            self.info(message)
            self.info('</chart>')
        else:
            self.info(gen_chars(10, '-'))
            if type in ['pie']:
                total = sum([i[0] for i in message['subsets']]) if 'total' not in message else message['total'][0]
                max_len = max([len(i[1]) for i in message['subsets']])
                len_format = " {: >" + str(max_len) + "}: "

                if 'label' in message:
                    label = message['label']
                    self.info('{label}'.format(label=label))
                for i in message['subsets']:
                    p = 100.0*i[0]/total
                    l = int(p/5)
                    info_str =len_format.format(i[1]) + "[{:-<20}]".format(gen_chars(l,'#')) + ' {val} ({p}% of Total)'.format(label=str(i[1]), val=i[0], p=format(p, '.2f'))
                    self.info(info_str)
                if 'total' in message:
                    label = '{label} ({count})'.format(label=message['total'][1], count=message['total'][0])
                    self.info(' {label}'.format(label=label))

            if type in ['histogram']:

                total = sum(message['y'])
                max_len = max([len(str(i)) for i in message['x']])
                len_format = " {: >" + str(max_len) + "}: "
                max_val = max(message['y'])

                if 'label' in message:
                    label = message['label']
                    self.info('{label}'.format(label=label))

                for i,v in enumerate(message['y']):
                    p = 100.0 * v / max_val
                    prob = 100.0 *v /total
                    l = int(p / 5)
                    info_str = len_format.format(message['x'][i]) + "[{: <20}".format(
                        gen_chars(l, '#')) + '  ({p}% likely)'.format(label=message['x'][i], p=format(prob, '.2f'))
                    self.info(info_str)



            elif type in ['list']:
                max_len = max([len(i) for i in message.keys()])
                len_format = " {: >" + str(max_len) + "}: "
                for key in message:
                    self.info(len_format.format(key) + '{val}'.format(val=message[key]))
            else:
                self.info(message)
                self.info('info type: {type}'.format(type=type))
            self.info(gen_chars(10, '-'))



main_logger_uuid = 'core-logger'
log = MindsdbLogger(log_level=CONFIG.DEFAULT_LOG_LEVEL, log_url=None, send_logs=False, uuid=main_logger_uuid)
