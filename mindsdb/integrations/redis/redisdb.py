import walrus

from mindsdb.integrations.base import StreamIntegration
from mindsdb.streams import RedisStream


class RedisConnectionChecker:
    def __init__(self, **params):
        self.connection_info = {
            'host': params['host'],
            'port': params['port'],
            'password': params['password'],
        }

    def check_connection(self):
        try:
            client = walrus.Database(**self.connection_info)
            client.dbsize()
        except Exception:
            return False
        else:
            return False


class Redis(StreamIntegration):
    def __init__(self, config, name, db_info):
        StreamIntegration.__init__(self, config, name)
        self.connection_info = {
            'host': db_info['host'],
            'port': db_info['port'],
            'password': db_info['password'],
        }
    
    def _make_stream(self, s):
        return RedisStream(
            s.name,
            s.predictor,
            self.connection_info,
            s.stream_in,
            s.stream_out,
            s.stream_anomaly
        )
