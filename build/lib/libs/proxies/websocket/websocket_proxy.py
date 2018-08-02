from autobahn.twisted.websocket import WebSocketServerProtocol, WebSocketServerFactory

import logging as logging_real
from libs.helpers.logging import logging

from libs.helpers.json_helpers import get_json_data, json_to_string,is_json
from libs.controllers.session_controller import SessionController
import config


def is_valid_auth(data):
    if data['user'] == 'root' and data['password'] == 'test1':
        return True
    return False

class WebsocketProxy(WebSocketServerProtocol):
    sessionVariables = {}
    current_transaction = False
    def onConnect(self, request):
        key = request.peer
        logging_real.info('new client connected: ' + key)
        self.sessionVariables[key] = {
            'MessageManager': self,
            'Auth': False,
            'Session': SessionController()
        }

    def onMessage(self, payload, isBinary):
        if not payload:
            return
        if not is_json(payload):
            message = {'error': True, 'errorMsg': 'No JSON data received.'}
            logging.error(message)
            return

        data = get_json_data(payload)
        manager = self.sessionVariables[self.peer]['MessageManager']
        session = self.sessionVariables[self.peer]['Session']
        if self.sessionVariables[self.peer]['Auth']:
            if 'id' in data:
                if 'query' in data:
                    self.current_transaction = session.newTransaction(sql_query=data['query'])
                    message = {'error': self.current_transaction.error, 'errorMsg':self.current_transaction.errorMsg, 'data': self.current_transaction.output_data_array, 'meta':self.current_transaction.output_metadata, 'id':data['id']}
                    manager.sendMessage(json_to_string(message), False)
                else:
                    message = {'error': True, 'errorMsg': 'No query received.'}
                    manager.sendMessage(json_to_string(message), False)
            else:
                message = {'error':True,'errorMsg':'No id received.'}
                manager.sendMessage(json_to_string(message), False)
        else:
            if 'auth' in data:
                if 'user' in data['auth'] and 'password' in data['auth']:
                    if is_valid_auth(data['auth']):
                        self.sessionVariables[self.peer]['Auth'] = True
                        ret = {}
                        for i in vars(config):
                            if i.isupper():
                                ret[i] = getattr(config,i)
                        message = {'error': False, 'data': 'Auth Ok','config':ret}
                        manager.sendMessage(json_to_string(message), False)
                    else:
                        message = {'error': True, 'errorMsg': 'Invalid user or password'}
                        manager.sendMessage(json_to_string(message), False)
                else:
                    message = {'error': True, 'errorMsg': 'No user or password received.'}
                    manager.sendMessage(json_to_string(message), False)
            else:
                message = {'error': True, 'errorMsg': 'Not authorized.'}
                manager.sendMessage(json_to_string(message), False)
        return

    def onClose(self, wasClean, code, reason):
        if self.peer in self.sessionVariables:
            del self.sessionVariables[self.peer]
            logging_real.info(self.peer + ' variables removed')
        return





    @staticmethod
    def startProxy(reactor):
        logging_real.info('Starting MindsDB Websocket proxy server on {config}'.format(config=config.WEBSOCKET_URL))
        port = int(config.WEBSOCKET_URL.split(':')[-1])
        factory = WebSocketServerFactory(config.WEBSOCKET_URL)
        factory.protocol = WebsocketProxy
        reactor.listenTCP(port, factory)




if __name__ == '__main__':

    logging.basicConfig(**config.PROXY_LOG_CONFIG)
    WebsocketProxy.startProxy()


