from mindsdb.api.mongo.classes import Responder
# import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    def when(self, query):
        return (
            ('buildinfo' in query or 'buildInfo' in query)
            and '$db' in query
            and (query['$db'] == 'test' or query['$db'] == 'admin')
        )

    result = {
        "version": "MindsDB 2.6",   # TODO set real
        "ok": 1
    }


responder = Responce()
