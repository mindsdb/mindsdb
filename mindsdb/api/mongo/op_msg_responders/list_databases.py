from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'listDatabases': helpers.is_true}

    result = {
        'databases': [{
            'name': 'mindsdb',
            'sizeOnDisk': 1 << 16,
            'empty': False
        }, {
            'name': 'admin',
            'sizeOnDisk': 1 << 16,
            'empty': False
        }],
        'ok': 1,
    }


responder = Responce()
