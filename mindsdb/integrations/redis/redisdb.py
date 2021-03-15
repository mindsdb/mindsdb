import redis
from mindsdb.integrations.base import Integration


class RedisConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port', 6379)
        self.database = kwargs.get('database', 0)
        self.user = kwargs.get('user', None)
        self.password = kwargs.get('password', None)

    def _get_connection(self):
        return redis.client.Redis(host=self.host, port=self.port,
                                  socket_connect_timeout=10)

    def check_connection(self):
        client = self._get_connection()
        try:
            client.dbsize()
            return True
        except Exception:
            return False


class Redis(Integration, RedisConnectionChecker):
    def __init__(self, config, name):
        super().__init__(config, name)
        intergration_info = self.config['integrations'][self.name]
        self.host = intergration_info.get('host')
        self.port = intergration_info.get('port', 6379)
        self.db = intergration_info.get('database', 0)
        self.input_stream = intergration_info.get('stream')
        self.user = intergration_info.get('user', None)
        self.password = intergration_info.get('password', None)

    def setup(self):
        pass

    def _query(self):
        pass

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass
