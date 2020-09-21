from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'replSetGetStatus': helpers.is_true}

    result = {
        "ok": 0
    }


responder = Responce()
