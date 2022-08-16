from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.api.mongo.classes import Responder


class Responce(Responder):
    def when(self, query):
        return (
            'buildinfo' in query or 'buildInfo' in query
        )

    result = {
        'version': '3.6.8',
        'versionArray': [3, 6, 8, 0],
        'ok': 1
    }


responder = Responce()
