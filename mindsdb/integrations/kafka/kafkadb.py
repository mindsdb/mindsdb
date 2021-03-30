import os
import json
import kafka

from threading import Thread
from mindsdb.utilities.config import STOP_THREADS_EVENT
from mindsdb.utilities.log import log
from mindsdb.integrations.base import Integration


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
        # self.contorl_stream = 
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.streams = {}
        self.stop_event = STOP_THREADS_EVENT

    def setup(self):

        self.start()

    def start(self):
        Thread(target=Kafka.work, args=(self, )).start()

    def work(self):
        self.consumer = kafka.KafkaConsumer(bootstrap_servers=f"{self.host}:{self.port}")
        self.consumer.subscribe([self.control_topic_name])
        log.debug(f"Integration {self.name}: subscribed  to {self.control_stream_name} kafka topic")
        while not self.stop_event.wait(0.5):
            msg_str = next(self.consumer)
            msg = json.loads(msg_str)
            log.error(f"got next msg: {msg}")




