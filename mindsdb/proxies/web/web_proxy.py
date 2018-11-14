import os
import _thread

from flask import Flask, send_from_directory
import socketio
import eventlet

import logging as logging_real
import mindsdb.config as config

from mindsdb.libs.helpers.logging import logging
from mindsdb.libs.helpers.json_helpers import json_to_string

import time
import json
import traceback

class WebProxy:

    def startWebServer(self, test_config=None):
        # create and configure the app
        app = Flask(__name__, instance_relative_config=True)

        @app.route('/js/<path:path>')
        def send_js(path):
            return send_from_directory('static/js', path, cache_timeout=-1)

        @app.route('/jsx/<path:path>')
        def send_jsx(path):
            return send_from_directory('static/jsx', path, cache_timeout=-1)

        @app.route('/css/<path:path>')
        def send_css(path):
            return send_from_directory('static/css', path, cache_timeout=-1)

        @app.route('/img/<path:path>')
        def send_img(path):
            return send_from_directory('static/img', path, cache_timeout=-1)

        @app.route('/')
        def root():
            print('aaaaa')
            r = open('static/index.html')
            vars_to_expose = ['WEBSOCKET_URL', 'LOGGING_WEBSOCKET_URL']
            text = r.read()
            config_vars = json.dumps({var_name: value for var_name, value in vars(config).items() if var_name in vars_to_expose})
            text = text.format(ts_var=time.time(), config_vars=config_vars)

            return text

        return app


    def startWebSocketServer(self):
        sio = socketio.Server()


        @sio.on('connect')
        def connect(sid, environ):

            sio.emit('reply', room=sid)

        @sio.on('call')  # , namespace='/chat')
        def disconnect(sid, payload):
            try:
                service = payload['service']
                data = payload['data']
                uid = payload['uid']

                if hasattr(self.controller, service):
                    method = getattr(self.controller, service)
                    if data in [False]:
                        resp = method()
                    else:
                        resp = method(**data)
                    sio.emit('call_response', {'uid':uid, 'response': resp})
                else:
                    sio.emit('call_response', {'uid':uid, 'response': None, 'error': 'Service Not-found'})

            except:
                logging_real.error('something went wrong, check payload')
                logging_real.error(traceback.format_exc())

        @sio.on('disconnect')#, namespace='/chat')
        def disconnect(sid):
            print('disconnect ', sid)


        return sio


    def startProxy(self):

        logging_real.info('Starting MindsDB webserver ... ')
        app = self.startWebServer()
        print('aaaaaa')
        logging_real.info('Starting MindsDB Logging websocket server on {config}'.format(config=config.LOGGING_WEBSOCKET_URL))
        port = int(config.LOGGING_WEBSOCKET_URL.split(':')[-1])
        sio = self.startWebSocketServer()

        app = socketio.Middleware(sio, app)

        # deploy as an eventlet WSGI server
        #app.run()
        eventlet.wsgi.server(eventlet.listen(('', port)), app)


    def __init__(self, controller=None):
        self.controller = controller



if __name__ == '__main__':
    from mindsdb import MindsDB

    logging_real.basicConfig()
    logging_real.getLogger().setLevel(logging_real.DEBUG)
    controller = MindsDB()
    info_server = WebProxy(controller)
    info_server.startProxy()

    a = input('exit?')


