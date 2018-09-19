import os
import _thread
from flask import Flask
from autobahn.twisted.websocket import WebSocketServerProtocol, WebSocketServerFactory

import logging as logging_real
import mindsdb.config

from mindsdb.libs.helpers.logging import logging
from mindsdb.libs.helpers.json_helpers import json_to_string



def startWebServer(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)


    @app.route('/')
    def root():
        r = open('static/index.html')
        return r.read()

    app.run()




class LoggingWebsocketProxy(WebSocketServerProtocol):
    connections = {}
    def onConnect(self, request):
        key = request.peer
        self.connections[key] = {'MessageManager':self}
        logging_real.info('new client connected to logging websocket: ' + key)

    def broadCast(self, message):
        for key in self.connections:
            manager = self.connections[key]['MessageManager']
            manager.sendMessage(json_to_string(message), False)
        return

    def onMessage(self, payload, isBinary):
        return

    def onClose(self, wasClean, code, reason):
        if self.peer in self.connections:
            del self.connections[self.peer]
            logging_real.info(self.peer + ' variables removed')
        return


    @staticmethod
    def startProxy(reactor):
        logging_real.info('Starting MindsDB webserver ... ')
        _thread.start_new_thread(startWebServer, ())
        logging_real.info('Starting MindsDB Logging websocket server on {config}'.format(config=config.LOGGING_WEBSOCKET_URL))
        port = int(config.LOGGING_WEBSOCKET_URL.split(':')[-1])
        factory = WebSocketServerFactory(config.LOGGING_WEBSOCKET_URL)
        factory.protocol = LoggingWebsocketProxy
        reactor.listenTCP(port, factory)
        logging.registerWS(factory.protocol)




if __name__ == '__main__':
    from twisted.internet import reactor

    logging.basicConfig(**config.PROXY_LOG_CONFIG)
    LoggingWebsocketProxy.startProxy(reactor)


