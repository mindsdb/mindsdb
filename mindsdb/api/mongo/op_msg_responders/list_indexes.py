from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'listIndexes': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        return {
            'cursor': [{
                'v': 2,
                'key': {
                    '_id': 1
                },
                'name': '_id_',
                'ns': f"{query['$db']}.{query['listIndexes']}"
            }],
            'ok': 1,
        }


responder = Responce()
