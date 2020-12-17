from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'getLog': helpers.is_true}

    result = {
        "log": [],
        "ok": 1
    }


responder = Responce()
