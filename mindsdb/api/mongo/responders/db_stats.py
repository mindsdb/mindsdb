from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb.utilities.config import config


class Responce(Responder):
    when = {'dbStats': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        db = query['$db']
        collections = 0
        if db == config.get('default_project'):
            collections = 2 + len(mindsdb_env['model_controller'].get_models())
        return {
            'db': db,
            'collections': collections,
            'views': 0,
            'objects': 0,
            'avgObjSize': 0,
            'dataSize': 0,
            'storageSize': 0,
            'numExtents': 0,
            'indexes': 0,
            'indexSize': 0,
            'fileSize': 0,
            'fsUsedSize': 0,
            'fsTotalSize': 0,
            'ok': 1
        }


responder = Responce()
