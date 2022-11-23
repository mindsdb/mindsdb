from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'getParameter': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):

        if query['featureCompatibilityVersion']:
            res = {
                "featureCompatibilityVersion":
                    {
                        "version": "3.6"
                    },
                "ok": 1.0
            }
        else:
            raise NotImplementedError(f'Unknown parameter {query}')
        return res


responder = Responce()
