from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'aggregate': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = query['$db']
        collection = query['aggregate']

        # TODO use 'find' responder

        count = 0
        if db == 'mindsdb' and collection == 'predictors':
            count = len(mindsdb_env['model_controller'].get_models())

        return {
            'count': count,
            'maxTimeMs': 5000,
            '$db': db
        }


responder = Responce()
