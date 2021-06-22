import kafka

from mindsdb.integrations.base import StreamIntegration
import mindsdb.interfaces.storage.db as db
from mindsdb.streams import KafkaStream, StreamController


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
        StreamIntegration.__init__(
            self,
            config,
            name,
            control_stream=KafkaStream('control_stream_' + name, self.connection_info)
        )

    def _make_stream(self, s: db.Stream):
        return StreamController(
            s.name,
            s.predictor,
            stream_in=KafkaStream(s.stream_in, self.connection_info),
            stream_out=KafkaStream(s.stream_out, self.connection_info),
            anomaly_stream=KafkaStream(s.anomaly_stream, self.connection_info) if s.anomaly_stream is not None else None,
            learning_stream=KafkaStream(s.learning_stream, self.connection_info) if s.learning_stream is not None else None,
        )
