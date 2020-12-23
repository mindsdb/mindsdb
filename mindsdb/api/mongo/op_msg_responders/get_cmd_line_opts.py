from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'getCmdLineOpts': helpers.is_true}

    result = {
        'argv': [
            'mongod'
        ],
        'parsed': {
        },
        'ok': 1
    }


responder = Responce()
