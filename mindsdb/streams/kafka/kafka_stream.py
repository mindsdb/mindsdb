import json
from copy import deepcopy

import kafka

from mindsdb.utilities.json_encoder import CustomJSONEncoder
from mindsdb.streams.base.base_stream import BaseStream


class KafkaStream(BaseStream):
    def __init__(self, topic, connection_info, mode='rw'):
        self.topic = topic
        self.producer_kwargs = {'acks': 'all'}
        self.connection_info = deepcopy(connection_info)
        self.producer_kwargs.update(self.connection_info.get('advanced', {}).get('producer', {}))
        self.consumer_kwargs = {'consumer_timeout_ms': 1000}
        self.consumer_kwargs.update(self.connection_info.get('advanced', {}).get('consumer', {}))
        self.producer = None
        self.consumer = None

        if 'advanced' in self.connection_info:
            del self.connection_info['advanced']
        if 'w' in mode:
            self.producer = kafka.KafkaProducer(**self.connection_info, **self.producer_kwargs)
        if 'r' in mode:
            self.consumer = kafka.KafkaConsumer(**self.connection_info, **self.consumer_kwargs)
            self.consumer.subscribe(topics=[topic])

    def read(self):
        for msg in self.consumer:
            yield json.loads(msg.value)

    def write(self, dct):
        self.producer.send(self.topic, json.dumps(dct, cls=CustomJSONEncoder).encode('utf-8'))
        self.producer.flush()

    def __del__(self):
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
