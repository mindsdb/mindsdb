import json
from copy import deepcopy

import kafka

from mindsdb.interfaces.stream.base import StreamIntegration
import mindsdb.interfaces.storage.db as db
from mindsdb_streams import KafkaStream, StreamController, StreamLearningController


class KafkaConnectionChecker:
    def __init__(self, **params):
        self.connection_info = params['connection']

    def check_connection(self):
        try:
            client = kafka.KafkaClient(**self.connection_info)
        except Exception:
            return False
        else:
            client.close()
            return True


class Kafka(StreamIntegration, KafkaConnectionChecker):
    def __init__(self, config, name, db_info):
        self.connection_info = db_info['connection']

        # Back compatibility with initial API version
        self.control_stream = db_info.get('control_stream') or db_info.get('topic') or None
        if 'advanced' in db_info:
            self.connection_info['advanced'] = db_info['advanced']

        self.control_connection_info = deepcopy(self.connection_info)
        # don't need to read all records from the beginning of 'control stream'
        # since all active streams are saved in db. Use 'latest' auto_offset_reset for control stream
        if 'advanced' in self.control_connection_info:
            if 'consumer' in self.control_connection_info['advanced']:
                self.control_connection_info['advanced']['consumer']['auto_offset_reset'] = 'latest'

        StreamIntegration.__init__(
            self,
            config,
            name,
            control_stream=KafkaStream(self.control_stream, self.control_connection_info) if self.control_stream else None
        )

    def _make_stream(self, s: db.Stream):
        if s.learning_params and s.learning_threshold:
            learning_params = json.loads(s.learning_params) if isinstance(s.learning_params, str) else s.learning_params
            return StreamLearningController(
                s.name,
                s.predictor,
                learning_params,
                s.learning_threshold,
                stream_in=KafkaStream(s.stream_in, self.connection_info),
                stream_out=KafkaStream(s.stream_out, self.connection_info),
                in_thread=True
            )
        return StreamController(
            s.name,
            s.predictor,
            stream_in=KafkaStream(s.stream_in, self.connection_info),
            stream_out=KafkaStream(s.stream_out, self.connection_info),
            stream_anomaly=KafkaStream(s.anomaly_stream, self.connection_info) if s.anomaly_stream is not None else None,
            in_thread=True
        )
