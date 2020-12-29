from pymongo import MongoClient

from mindsdb.integrations.base import Integration


class MongoDB(Integration):
    def _query(self, query):
        return None

    def setup(self):
        pass

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass

    def check_connection(self):
        try:
            integration = self.config['integrations'][self.name]
            server = MongoClient(
                integration['host'],
                port=integration.get('port', 27017),
                username=integration['user'],
                password=integration['password'],
                serverSelectionTimeoutMS=5000
            )
            server.server_info()
            connected = True
        except Exception:
            connected = False
        return connected
