from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'whatsmyuri': helpers.is_true}

    def result(self, query, request_env, mindsdb_env):
        mongodb_config = mindsdb_env['config']['api']['mongodb']
        host = mongodb_config['host']
        port = mongodb_config['port']
        return {
            'you': f'{host}:{port}',
            'ok': 1
        }


responder = Responce()
