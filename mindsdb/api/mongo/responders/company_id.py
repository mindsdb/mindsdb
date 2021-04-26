from mindsdb.api.mongo.classes import Responder


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        company_id = query.get('company_id')

        # TODO change mindsdb_env according on company_id
        # mindsdb_env = {
        #     'config': config,
        #     'data_store': DataStore(),
        #     'mindsdb_native': NativeInterface()
        # }

        return None


responder = Responce()
