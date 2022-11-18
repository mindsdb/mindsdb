from mindsdb.api.mongo.classes import Responder
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.utilities.context import context as ctx


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        company_id = query.get('company_id')
        user_class = query.get('user_class', 0)
        need_response = query.get('need_response', False)

        mindsdb_env['user_class'] = user_class
        mindsdb_env['company_id'] = company_id

        ctx.company_id = company_id
        ctx.user_class = user_class

        for name in [
            'model_controller',
            'integration_controller',
            'project_controller',
            'database_controller'
        ]:
            mindsdb_env[name] = WithKWArgsWrapper(
                mindsdb_env[f'original_{name}'],
                company_id=company_id
            )

        if need_response:
            return {'ok': 1}

        return None


responder = Responce()
