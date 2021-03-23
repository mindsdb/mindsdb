from .responder import Responder


class RespondersCollection():
    def __init__(self):
        self.responders = []

    def find_match(self, query):
        for r in self.responders:
            if r.match(query):
                return r
        raise Exception(f'Is not responder for query: {query}')

    def add(self, when, result):
        self.responders.append(
            Responder(when, result)
        )
