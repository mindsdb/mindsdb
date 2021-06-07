import json
import kafka

from mindsdb.streams.base.base_stream import BaseStream


class KafkaStream(BaseStream):
    def __init__(self, name, predictor, connection_info, topic_in, topic_out):
        BaseStream.__init__(self, name, predictor)
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.consumer = kafka.KafkaConsumer(**connection_info)
        self.consumer.subscribe(topics=[self.topic_in])
        self.producer = kafka.KafkaProducer(**connection_info, acks='all')

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
