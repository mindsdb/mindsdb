from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'hostInfo': helpers.is_true}

    # NOTE answer is not full, but looks like this info is enough
    result = {
        'system': {
            'cpuAddrSize': 64,
            'memSizeMB': 15951,
            'numCores': 4,
            'cpuArch': "x86_64",
            'numaEnabled': False
        },
        'os': {
            'type': "Linux",
            'name': "Ubuntu",
            'version': "20.04"
        },
        'extra': {
        },
        'ok': 1
    }


responder = Responce()
