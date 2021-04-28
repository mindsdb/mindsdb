from mindsdb.api.mongo.classes import Responder
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterface, ModelInterfaceWrapper


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        company_id = query.get('company_id')

        mindsdb_env['company_id'] = company_id
        mindsdb_env['data_store'] = DataStore(company_id=company_id)
        mindsdb_env['mindsdb_native'] = ModelInterfaceWrapper(
            model_interface=mindsdb_env['mindsdb_native'],
            company_id=company_id
        )

        return None


responder = Responce()
