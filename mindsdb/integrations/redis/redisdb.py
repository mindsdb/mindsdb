import walrus

from mindsdb.integrations.base import StreamIntegration
from mindsdb.streams.redis.redis_stream import RedisStream


class RedisConnectionChecker:
    def __init__(self, connection_info):
        self.connection_info = connection_info

    def _get_connection(self):
        return walrus.Database(**self.connection_info)

    def check_connection(self):
        client = self._get_connection()
        try:
            client.dbsize()
            return True
        except Exception:
            return False


class Redis(StreamIntegration, RedisConnectionChecker):
    def __init__(self, config, name, db_info):
        StreamIntegration.__init__(self, config, name)
    
    def _make_stream(self, s):
        return RedisStream(s.name, s.predictor, s.connection_info, s.stream_in, s.stream_out, s.stream_anomaly)
