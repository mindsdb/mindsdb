from mindsdb.api.mongo.classes import Responder
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        company_id = query.get('company_id')
        user_class = query.get('user_class', 0)
        need_response = query.get('need_response', False)

        mindsdb_env['user_class'] = user_class
        mindsdb_env['company_id'] = company_id
        mindsdb_env['model_interface'] = WithKWArgsWrapper(
            mindsdb_env['origin_model_interface'],
            company_id=company_id
        )
        mindsdb_env['integration_controller'] = WithKWArgsWrapper(
            mindsdb_env['origin_integration_controller'],
            company_id=company_id
        )

        if need_response:
            return {'ok': 1}

        return None


responder = Responce()
