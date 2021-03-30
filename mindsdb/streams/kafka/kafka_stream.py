import os
import json
from threading import Thread, Event

import kafka
from kafka.admin import NewTopic

from mindsdb.utilities.log import log
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface

class KafkaStream(Thread):
    def __init__(self, host, port, topic_in, topic_out, predictor, _type):
        self.host = host
        self.port = port
        self.predictor = predictor
        self.stream_in_name = topic_in
        self.stream_out_name = topic_out
        self.consumer = kafka.KafkaConsumer(bootstrap_servers=f"{self.host}:{self.port}", consumer_timeout_ms=1000)
        self.consumer.subscribe(topics=[self.stream_in_name])
        self.producer = kafka.KafkaProducer(bootstrap_servers=f"{self.host}:{self.port}")
        self.admin = kafka.KafkaAdminClient(bootstrap_servers=f"{self.host}:{self.port}")
        try:
            self.topic = NewTopic(topic_out, num_partitions=1, replication_factor=1)
            self.admin.create_topics([self.topic])
        except kafka.errors.TopicAlreadyExistsError:
            pass
        self._type = _type
        self.native_interface = NativeInterface()
        self.format_flag = 'explain'

        self.stop_event = Event()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        super().__init__(target=KafkaStream.make_prediction, args=(self,))

    def make_prediction(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return
        while not self.stop_event.wait(0.5):
            try:
                msg_str = next(self.consumer)
                when_data = json.loads(msg_str)
                result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
                log.error(f"STREAM: got {result}")
                for res in result:
                    in_json = json.dumps(res)
