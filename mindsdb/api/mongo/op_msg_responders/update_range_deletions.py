from mindsdb.api.mongo.classes import Responder


class Responce(Responder):
    when = {'update': 'rangeDeletions'}

    result = {
        "ok": 1
    }


responder = Responce()
