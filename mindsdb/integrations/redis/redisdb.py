import json

import walrus

from mindsdb.integrations.base import StreamIntegration
import mindsdb.interfaces.storage.db as db
from mindsdb.streams import RedisStream, StreamController


class RedisConnectionChecker:
    def __init__(self, **params):
        self.connection_info = params['connection']

    def check_connection(self):
        if isinstance(self.connection_info, str):
            self.connection_info = json.loads(self.connection_info)
        try:
            client = walrus.Database(**self.connection_info)
            client.dbsize()
        except Exception:
            return False
        else:
            return True


class Redis(StreamIntegration, RedisConnectionChecker):
    def __init__(self, config, name, db_info):
        self.connection_info = db_info['connection']

        # Back compatibility with initial API version
        self.control_stream = db_info.get('control_stream') or db_info.get('stream') or None
        if 'advanced' in db_info:
            self.connection_info['advanced'] = db_info['advanced']

        StreamIntegration.__init__(
            self,
            config,
            name,
            control_stream=RedisStream(self.control_stream, self.connection_info) if self.control_stream else None
        )

    def _make_stream(self, s: db.Stream) -> StreamController:
        return StreamController(
            s.name,
            s.predictor,
            stream_in=RedisStream(s.stream_in, self.connection_info),
            stream_out=RedisStream(s.stream_out, self.connection_info),
            anomaly_stream=RedisStream(s.anomaly_stream, self.connection_info) if s.anomaly_stream is not None else None,
            learning_stream=RedisStream(s.learning_stream, self.connection_info) if s.learning_stream is not None else None,
        )
