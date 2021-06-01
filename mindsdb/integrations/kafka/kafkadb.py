import kafka

from mindsdb.integrations.base import StreamIntegration
from mindsdb.streams import KafkaStream


class KafkaConnectionChecker:
    def __init__(self, **params):
        self.connection_info = {
            'host': params['host'],
            'port': params['port'],
            'password': params['password'],
        }

    def check_connection(self):
        try:
            client = kafka.KafkaClient(**self.connection_info)
        except Exception:
            return False
        else:
            client.close()
            return True


class Kafka(StreamIntegration):
    def __init__(self, config, name, db_info):
        StreamIntegration.__init__(self, config, name)
        self.connection_info = {
            'host': db_info['host'],
            'port': db_info['port'],
            'password': db_info['password'],
        }

    def _make_stream(self, s):
        return KafkaStream(
            s.name,
            s.predictor,
            self.connection_info,
            s.topic_in,
            s.topic_out,
            s.topic_anomaly
        )
    