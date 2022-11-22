from mindsdb.api.mongo.classes import Responder
from mindsdb.utilities.context import context as ctx


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        company_id = query.get('company_id')
        user_class = query.get('user_class', 0)
        need_response = query.get('need_response', False)

        ctx.company_id = company_id
        ctx.user_class = user_class

        if need_response:
            return {'ok': 1}

        return None


responder = Responce()
