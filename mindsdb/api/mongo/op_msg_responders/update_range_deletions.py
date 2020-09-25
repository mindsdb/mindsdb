from mindsdb.api.mongo.classes import Responder


class Responce(Responder):
    when = {'update': 'rangeDeletions'}

    result = response = {
        "ok": 1
    }


responder = Responce()
