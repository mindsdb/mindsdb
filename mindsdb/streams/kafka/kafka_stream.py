import os
import json
from threading import Thread, Event

import kafka
from kafka.admin import NewTopic

from mindsdb.utilities.log import log
from mindsdb.streams.base.base_stream import StreamTypes
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface

class KafkaStream(Thread):
    def __init__(self, connection_info, advanced_info, topic_in, topic_out, predictor, _type, **ts_params):
        self.connection_info = connection_info
        self.advanced_info = advanced_info
        self.predictor = predictor
        self.stream_in_name = topic_in
        self.stream_out_name = topic_out
        self.consumer = kafka.KafkaConsumer(**self.connection_info, **self.advanced_info.get('consumer', {}))
        self.consumer.subscribe(topics=[self.stream_in_name])
        self.producer = kafka.KafkaProducer(**self.connection_info, **self.advanced_info.get('producer', {}))
        self.admin = kafka.KafkaAdminClient(**self.connection_info)
        try:
            self.topic = NewTopic(self.stream_out_name, num_partitions=1, replication_factor=1)
            self.admin.create_topics([self.topic])
        except kafka.errors.TopicAlreadyExistsError:
            pass
        self._type = _type
        self.native_interface = NativeInterface()
        self.format_flag = 'explain'

        self.stop_event = Event()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.caches = {}
        self.ts_params = ts_params
        if self._type.lower() == StreamTypes.timeseries:
            self.target = self.ts_params.get('target')
            self.window = self.ts_params.get('window_size')
            self.gb = self.ts_params.get('group_by')
            self.dt = self.ts_params.get('order_by')
            super().__init__(target=KafkaStream.make_timeseries_predictions, args=(self,))
        else:
            super().__init__(target=KafkaStream.make_prediction, args=(self,))

    def predict_ts(self, cache_name):
        when_list = [x for x  in self.caches[cache_name]]
        for x in when_list:
            if self.target not in x:
                x['make_predictions'] = False
            else:
                x['make_predictions'] = True

        result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_list)
        log.error(f"TIMESERIES STREAM: got {result}")
        for res in result:
            in_json = json.dumps(res)
            to_send = in_json.encode('utf-8')
            log.error(f"sending {to_send}")
            self.producer.send(self.stream_out_name, to_send)
        self.caches[cache_name] = self.caches[cache_name][1:]

    def make_prediction_from_cache(self, cache_name):
        cache = self.caches[cache_name]
        log.error("STREAM: in make_prediction_from_cache")
        if len(cache) >= self.window:
            log.error(f"STREAM: make_prediction_from_cache - len(cache) = {len(cache)}")
            self.predict_ts(cache_name)

    def to_cache(self, record):
        gb_val = record[self.gb]
        cache_name = f"cache.{gb_val}"
        if cache_name not in self.caches:
            cache = []
            self.caches[cache_name] = cache

        log.error(f"STREAM: cache {cache_name} has been created")
        self.make_prediction_from_cache(cache_name)
        self.handle_record(cache_name, record)
        self.make_prediction_from_cache(cache_name)
        log.error("STREAM in cache: current iteration has done.")

    def handle_record(self, cache_name, record):
        log.error(f"STREAM: handling cache {cache_name} and {record} record.")
        cache = self.caches[cache_name]
        cache.append(record)
        cache = self.sort_cache(cache)
        self.caches[cache_name] = cache

    def sort_cache(self, cache):
        return sorted(cache, key=lambda x: x[self.dt])

    def make_timeseries_predictions(self):
        log.error("STREAM: running 'make_timeseries_predictions'")
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return

        while not self.stop_event.wait(0.5):
            try:
                msg_str = next(self.consumer)
                when_data = json.loads(msg_str.value)
                self.to_cache(when_data)
            except StopIteration:
                pass

        log.error("Stopping stream..")
        self.producer.close()
        self.consumer.close()
        session.close()

    def make_prediction(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return
        while not self.stop_event.wait(0.5):
            try:
                msg_str = next(self.consumer)
                when_data = json.loads(msg_str.value)
                result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
                log.error(f"STREAM: got {result}")
                for res in result:
                    in_json = json.dumps({"prediction": res})
                    to_send = in_json.encode('utf-8')
                    log.error(f"sending {to_send}")
                    self.producer.send(self.stream_out_name, to_send)
            except StopIteration:
                pass
        log.error("Stopping stream..")
        self.producer.close()
        self.consumer.close()
        session.close()
