from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'getFreeMonitoringStatus': helpers.is_true}

    result = {
        'state': 'undecided',
        'ok': 1
    }


responder = Responce()
