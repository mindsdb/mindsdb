import os
import json
import kafka

from threading import Thread
from mindsdb.utilities.config import STOP_THREADS_EVENT
from mindsdb.utilities.log import log
from mindsdb.integrations.base import Integration
from mindsdb.streams.kafka.kafka_stream import KafkaStream

class KafkaConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port', 9092)

    def _get_connection(self):
        return kafka.KafkaAdminClient(bootstrap_servers=f"{self.host}:{self.port}")
    def check_connection(self):
        try:
            client = self._get_connection()
            client.close()
            return True
        except Exception:
            return False


class Kafka(Integration, KafkaConnectionChecker):
    def __init__(self, config, name):
        Integration.__init__(self, config, name)
        intergration_info = self.config['integrations'][self.name]
        self.host = intergration_info.get('host')
        self.port = intergration_info.get('port', 9092)
        self.control_topic_name = intergration_info.get('topic')
        self.client = self._get_connection()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.streams = {}
        self.stop_event = STOP_THREADS_EVENT
        log.error(f"Integration: name={self.name}, host={self.host}, port={self.port}, control_topic={self.control_topic_name}")

    def setup(self):
        self.start()

    def start(self):
        Thread(target=Kafka.work, args=(self, )).start()

    def work(self):
        # self.consumer = kafka.KafkaConsumer(bootstrap_servers=f"{self.host}:{self.port}", consumer_timeout_ms=1000)
        # self.consumer = kafka.KafkaConsumer(bootstrap_servers=f"{self.host}:{self.port}")
        self.consumer = kafka.KafkaConsumer(bootstrap_servers="127.0.0.1:9092")
        self.consumer.subscribe([self.control_topic_name])
        log.error(f"Integration {self.name}: subscribed  to {self.control_topic_name} kafka topic")
        # while not self.stop_event.wait(0.5):
        while True:
            try:
                log.error("waiting new messages from control topic")
                msg_str = next(self.consumer)
                log.error(f"got next raw_msg: {msg_str.value}")
                stream_params = json.loads(msg_str.value)
                log.error(f"got next msg: {stream_params}")
                stream = self.get_stream_from_kwargs(**stream_params)
                stream.start()
            except StopIteration as e:
                log.error(f"error: {e}")
        self.consumer.close()

    def get_stream_from_kwargs(self, **kwargs):
        topic_in = kwargs.get('input_stream')
        topic_out = kwargs.get('output_stream')
        predictor_name = kwargs.get('predictor')
        stream_type = kwargs.get('type', 'forecast')
        return KafkaStream(self.host, self.port,
                           topic_in, topic_out,
                           predictor_name, stream_type)


    def _query(self):
        pass

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass
