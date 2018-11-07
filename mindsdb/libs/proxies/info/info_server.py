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

def startWebServer(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    @app.route('/js/<path:path>')
    def send_js(path):
        return send_from_directory('static/js', path, cache_timeout=-1)

    @app.route('/css/<path:path>')
    def send_css(path):
        return send_from_directory('static/css', path, cache_timeout=-1)

    @app.route('/img/<path:path>')
    def send_img(path):
        return send_from_directory('static/img', path, cache_timeout=-1)

    @app.route('/')
    def root():
        r = open('static/index.html')
        vars_to_expose = ['WEBSOCKET_URL', 'LOGGING_WEBSOCKET_URL']
        text = r.read()
        config_vars = json.dumps({var_name: value for var_name, value in vars(config).items() if var_name in vars_to_expose})
        text = text.format(ts_var=time.time(), config_vars=config_vars)

        return text

    return app


def startWebSocketServer():
    sio = socketio.Server()

    @sio.on('connect')
    def connect(sid, environ):
        print("connect ------------------------------------", sid)
        sio.emit('reply', room=sid)


    @sio.on('disconnect')   #, namespace='/chat')
    def disconnect(sid):
        print('disconnect ', sid)


    return sio


def startProxy():

    logging_real.info('Starting MindsDB webserver ... ')
    app = startWebServer()
    logging_real.info('Starting MindsDB Logging websocket server on {config}'.format(config=config.LOGGING_WEBSOCKET_URL))
    port = int(config.LOGGING_WEBSOCKET_URL.split(':')[-1])
    sio = startWebSocketServer()

    app = socketio.Middleware(sio, app)

    # deploy as an eventlet WSGI server
    #app.run(port=config.LOGGING_WEB_PORT)
    eventlet.wsgi.server(eventlet.listen(('', port)), app)




if __name__ == '__main__':

    logging_real.basicConfig()
    logging_real.getLogger().setLevel(logging_real.DEBUG)

    startProxy()
    a = input('exit?')


