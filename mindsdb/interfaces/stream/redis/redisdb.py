import json

import walrus

from mindsdb.interfaces.stream.base import StreamIntegration
import mindsdb.interfaces.storage.db as db
from mindsdb_streams import RedisStream, StreamController, StreamLearningController


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
        connection_data = db_info['connection_data']
        self.connection_info = connection_data['connection']

        # Back compatibility with initial API version
        self.control_stream = connection_data.get('control_stream') or connection_data.get('stream') or None
        if 'advanced' in connection_data:
            self.connection_info['advanced'] = connection_data['advanced']

        StreamIntegration.__init__(
            self,
            config,
            name,
            control_stream=RedisStream(self.control_stream, self.connection_info) if self.control_stream else None
        )

    def _make_stream(self, s: db.Stream):
        if s.learning_params and s.learning_threshold:
            learning_params = json.loads(s.learning_params) if isinstance(s.learning_params, str) else s.learning_params
            return StreamLearningController(
                s.name,
                s.predictor,
                learning_params,
                s.learning_threshold,
                stream_in=RedisStream(s.stream_in, self.connection_info),
                stream_out=RedisStream(s.stream_out, self.connection_info),
                in_thread=True
            )

        return StreamController(
            s.name,
            s.predictor,
            stream_in=RedisStream(s.stream_in, self.connection_info),
            stream_out=RedisStream(s.stream_out, self.connection_info),
            stream_anomaly=RedisStream(s.anomaly_stream, self.connection_info) if s.anomaly_stream is not None else None,
            in_thread=True
        )
