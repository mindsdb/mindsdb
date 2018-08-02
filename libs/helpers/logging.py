
class Logging:
    ws = False

    def info(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            print(msg)

    def warning(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            print(msg)

    def error(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            print(msg)

    def debug(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            print(msg)

    def critical(self, msg):
        if self.ws:
            self.ws.broadCast(self.ws, msg)
            return
        else:
            print(msg)

    def basicConfig(self,**args):
        return

    def registerWS(self,ws):
        self.ws = ws

logging = Logging()