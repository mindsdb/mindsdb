from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'saslStart': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        try:
            payload = query['payload'].decode()
            session.init_scram()
            responce = session.scram.process_client_first_message(payload)

            res = {
                'conversationId': 1,
                'done': False,
                'payload': responce.encode(),
                'ok': 1
            }
        except Exception:
            res = {
                'ok': 0
            }
        return res


responder = Responce()
