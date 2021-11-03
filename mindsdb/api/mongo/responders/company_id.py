from mindsdb.api.mongo.classes import Responder
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        company_id = query.get('company_id')
        need_response = query.get('need_response', False)

        mindsdb_env['company_id'] = company_id
        mindsdb_env['data_store'] = WithKWArgsWrapper(
            mindsdb_env['origin_data_store'],
            company_id=company_id
        )
        mindsdb_env['mindsdb_native'] = WithKWArgsWrapper(
            mindsdb_env['origin_model_interface'],
            company_id=company_id
        )
        mindsdb_env['datasource_controller'] = WithKWArgsWrapper(
            mindsdb_env['origin_datasource_controller'],
            company_id=company_id
        )

        if need_response:
            return {'ok': 1}

        return None


responder = Responce()
