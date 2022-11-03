from mindsdb.api.mongo.utilities import logger

from .responder import Responder


class RespondersCollection():
    def __init__(self):
        self.responders = []

    def find_match(self, query):
        for r in self.responders:
            if r.match(query):
                return r

        msg = f'Is not responder for query: {query}'

        class ErrorResponder(Responder):
            when = {}

            result = {
                "ok": 0.0,
                "errmsg": msg,
                "code": 59,
                "codeName": "CommandNotFound"
            }

        logger.error(msg)
        return ErrorResponder()

    def add(self, when, result):
        self.responders.append(
            Responder(when, result)
        )
