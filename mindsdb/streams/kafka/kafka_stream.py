import json
import kafka

from mindsdb.streams.base.base_stream import BaseStream


class KafkaStream(BaseStream):
    def __init__(self, name, predictor, connection_info, topic_in, topic_out, learning_topic=None):
        BaseStream.__init__(self, name, predictor)
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.consumer = kafka.KafkaConsumer(**connection_info)
        self.consumer.subscribe(topics=[topic_in])
        self.producer = kafka.KafkaProducer(**connection_info, acks='all')
        if learning_topic is None:
            self.learning_consumer = None
        else:
            self.learning_consumer = kafka.KafkaConsumer(**connection_info)
            self.learning_consumer.subscribe(topics=[learning_topic])

    def _read_from_learning_stream(self):
        print('reading from learning_stream')
        while True:
            try:
                msg = next(self.learning_consumer)
            except StopIteration:
                return
            else:
                yield json.loads(msg.value)
        # for msg in self.learning_consumer:
        #     yield json.loads(msg.value)
        
    def _get_learning_stream_size(self):
        return self.learning_consumer.position()

    def _read_from_in_stream(self):
        print('reading from stream_in')
        while True:
            try:
                msg = next(self.consumer)
            except StopIteration:
                return
            else:
                yield json.loads(msg.value)
        # for msg in self.consumer:
        #     yield json.loads(msg.value)

    def _write_to_out_stream(self, dct):
        print('writing to stream_out')
        self.producer.send(self.topic_out, json.dumps(dct).encode('utf-8'))
    
    def __del__(self):
        self.consumer.close()
        self.producer.close()
