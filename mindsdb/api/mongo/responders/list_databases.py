from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'listDatabases': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = mindsdb_env['config']['api']['mongodb']['database']

        return {
            'databases': [{
                'name': db,
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
