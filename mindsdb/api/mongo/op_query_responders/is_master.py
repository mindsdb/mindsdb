from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'isMaster': helpers.is_true}

    result = response = {
        "ismaster": True,
        "minWireVersion": 0,
        "maxWireVersion": 9,  # 6 - 3.6, 9 - 4.4
        "ok": 1
    }


responder = Responce()
