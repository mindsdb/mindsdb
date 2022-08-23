from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
import datetime as dt


class Responce(Responder):
    when = {'isMaster': helpers.is_true}

    result = {
        "ismaster": True,
        "localTime": dt.datetime.now(),
        "logicalSessionTimeoutMinutes": 30,
        "minWireVersion": 0,
        "maxWireVersion": 6,
        "readOnly": False,
        "ok": 1.0
    }


responder = Responce()
