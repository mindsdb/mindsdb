import pprint
import logging as true_logging

def LOG(msg, type):
    logger = getattr(true_logging, type)
    logging.basicConfig(level=true_logging.INFO)
    msg = pprint.pformat(str(msg))
    logger(msg)

class Logging:
    ws = False

    def info(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            print(msg)
            LOG(msg, 'info')

    def warning(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            LOG(msg, 'warning')

    def error(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            LOG(msg, 'error')

    def debug(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            LOG(msg, 'debug')

    def critical(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            LOG(msg,'critical')



    def basicConfig(self,**args):
        return

    def registerWS(self,ws):
        self.ws = ws

logging = Logging()