from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'_recvChunkStart': helpers.is_true}

    result = {
        "ok": 0
    }


responder = Responce()
