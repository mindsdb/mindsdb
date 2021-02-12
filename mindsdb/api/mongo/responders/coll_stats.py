from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'collStats': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = query['$db']
        collection = query['collStats']

        # NOTE real answer is huge, i removed most data from it.
        res = {
            'ns': "db.collection",
            'size': 1,
            'count': 0,
            'avgObjSize': 1,
            'storageSize': 16384,
            'capped': False,
            'wiredTiger': {
            },
            'nindexes': 1,
            'indexDetails': {
            },
            'totalIndexSize': 16384,
            'indexSizes': {
                '_id_': 16384
            },
            'ok': 1
        }

        res['ns'] = f"{db}.{collection}"
        if db == 'mindsdb' and collection == 'predictors':
            res['count'] = len(mindsdb_env['mindsdb_native'].get_models())

        return res


responder = Responce()
