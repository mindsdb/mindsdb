import uuid

from bson.int64 import Int64

from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'listCollections': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        models = mindsdb_env['mindsdb_native'].get_models()
        models = [x['name'] for x in models if x['status'] == 'complete']
        models += ['predictors', 'commands']
        cursor = {
            'id': Int64(0),  # should we save id somethere?
            'ns': 'qwe.$cmd.listCollections',
            'firstBatch': []
        }
        for i, name in enumerate(models):
            cursor['firstBatch'].append({
                'name': name,
                'type': 'collection',
                'options': {},
                'info': {
                    'readOnly': False,
                    'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, name).bytes,
                },
                'idIndex': {
                    'v': 2,
                    'key': {
                        '_id': 1
                    },
                    'name': '_id_',
                    'ns': f'qwe.{name}'
                }
            })
        return {
            'cursor': cursor,
            'ok': 1,
        }


responder = Responce()
