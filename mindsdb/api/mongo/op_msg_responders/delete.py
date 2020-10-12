from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'delete': helpers.is_true}

    def result(self, query, request_env, mindsdb_env):
        try:
            res = self._result(query, request_env, mindsdb_env)
        except Exception as e:
            res = {
                'n': 0,
                'writeErrors': [{
                    'index': 0,
                    'code': 0,
                    'errmsg': str(e)
                }],
                'ok': 1
            }
        return res

    def _result(self, query, request_env, mindsdb_env):
        table = query['delete']
        if table != 'predictors':
            raise Exception("Only REMOVE from 'predictors' collection is supported at this time")

        if len(query['deletes']) != 1:
            raise Exception("Should be only one argument in REMOVE operation")

        delete_filter = query['deletes'][0]['q']
        if len(delete_filter) != 1 or 'name' not in delete_filter:
            raise Exception("For db.predictors.delete operation only argumet of the form {name: 'predictor_name'} are supported")

        predictor_name = query['deletes'][0]['q']['name']

        models = mindsdb_env['mindsdb_native'].get_models()
        model_names = [x['name'] for x in models]

        n = 0
        if predictor_name in model_names:
            n = 1
            mindsdb_env['mindsdb_native'].delete_model(predictor_name)

        return {
            'n': n,
            'ok': 1
        }


responder = Responce()
