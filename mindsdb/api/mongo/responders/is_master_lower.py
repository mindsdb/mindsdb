from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'ismaster': helpers.is_true}

    result = {
        "ismaster": True,
        "minWireVersion": 0,
        "maxWireVersion": 9,
        "ok": 1
    }


responder = Responce()
