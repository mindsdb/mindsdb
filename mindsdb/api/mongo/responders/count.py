from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'count': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        collection = query['count']

        count = 0
        if collection == 'predictors':
            count = len(mindsdb_env['model_controller'].get_models())

        return {
            'n': count,
            'ok': 1
        }


responder = Responce()
