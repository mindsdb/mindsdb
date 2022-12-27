from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.utilities import logger


class Responce(Responder):
    when = {'saslStart': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        try:
            payload = query['payload'].decode()
            mechanism = query.get('mechanism', 'SCRAM-SHA-1')
            if mechanism == 'SCRAM-SHA-1':
                session.init_scram('sha1')
            elif mechanism == 'SCRAM-SHA-256':
                session.init_scram('sha256')
            responce = session.scram.process_client_first_message(payload)

            res = {
                'conversationId': 1,
                'done': False,
                'payload': responce.encode(),
                'ok': 1
            }
        except Exception as e:
            logger.warning(str(e))
            res = {
                'errmsg': str(e),
                'ok': 0
            }
        return res


responder = Responce()
