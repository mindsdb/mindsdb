from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.api.mongo.classes import Responder


class Responce(Responder):
    def when(self, query):
        return (
            ('buildinfo' in query or 'buildInfo' in query)
            and '$db' in query
            and (query['$db'] == 'test' or query['$db'] == 'admin')
        )

    result = {
        'version': f'MindsDB {mindsdb_version}',   # TODO set real
        'ok': 1
    }


responder = Responce()
