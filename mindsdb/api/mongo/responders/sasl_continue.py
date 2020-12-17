from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'saslContinue': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        try:
            payload = query['payload'].decode()

            if len(payload) > 0:
                responce = session.scram.process_client_second_message(payload)
                responce = responce.encode()
                done = False
            else:
                responce = None
                done = True

            res = {
                'conversationId': 1,
                'done': done,
                'payload': responce,
                'ok': 1
            }
        except Exception:
            res = {
                'ok': 0
            }
        return res


responder = Responce()
