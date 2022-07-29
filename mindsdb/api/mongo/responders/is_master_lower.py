from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers

from .is_master import responder as ismaster_responder


class Responce(Responder):
    when = {'ismaster': helpers.is_true}

    result = ismaster_responder.result


responder = Responce()
