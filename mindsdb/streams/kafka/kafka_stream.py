import json
from threading import Thread

import kafka

from mindsdb.utilities.log import log
from mindsdb.utilities.cache import Cache
from mindsdb.streams.base.base_stream import BaseStream
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor


class KafkaStream(BaseStream):
    def __init__(self, name, predictor, connection_info, topic_in, topic_out, topic_anomaly):
        BaseStream.__init__(self, name, predictor)
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.topic_anomaly = topic_anomaly
        self.consumer = kafka.KafkaConsumer(**connection_info)
        self.consumer.subscribe(topics=[self.topic_in])
        self.producer = kafka.KafkaProducer(**connection_info, acks='all')

    def _read_from_in_stream(self):
        for msg in self.consumer:
            yield json.loads(msg.value)

    def _write_to_out_stream(self, dct):
        self.producer.send(self.topic_out, json.dumps(dct).encode('utf-8'))
    
    def _write_to_anomaly_stream(self, dct):
        self.producer.send(self.topic_anomaly, json.dumps(dct).encode('utf-8'))

    def __del__(self):
        self.consumer.close()
        self.producer.close()