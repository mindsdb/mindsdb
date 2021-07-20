import json
from threading import Thread

import kafka

from mindsdb.utilities.log import log
from mindsdb.utilities.cache import Cache
from mindsdb.streams.base.base_stream import StreamTypes, BaseStream
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor


class KafkaStream(Thread, BaseStream):
    def __init__(self, name, connection_info, advanced_info, topic_in, topic_out, topic_anomaly, predictor, _type):
        self.stream_name = name
        self.connection_info = connection_info
        self.advanced_info = advanced_info
        self.predictor = predictor
        self.stream_in_name = topic_in
        self.stream_out_name = topic_out
        self.stream_anomaly_name = topic_anomaly
        self.consumer = kafka.KafkaConsumer(**self.connection_info, **self.advanced_info.get('consumer', {}))
        self.consumer.subscribe(topics=[self.stream_in_name])
        self.producer = kafka.KafkaProducer(**self.connection_info, **self.advanced_info.get('producer', {}), acks='all')

        BaseStream.__init__(self)
        self._type = _type

        if self._type.lower() == StreamTypes.timeseries:
            super().__init__(target=KafkaStream.make_timeseries_predictions, args=(self,))
        else:
            super().__init__(target=KafkaStream.make_prediction, args=(self,))
            self.cache = None

    def predict_ts(self, group_by):
        with self.cache:
            when_list = self.cache[group_by]
            for x in when_list:
                if self.target not in x:
                    x['make_predictions'] = False
                else:
                    x['make_predictions'] = True
            result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_list)
            log.debug(f"TIMESERIES STREAM {self.stream_name}: got {result}")
            for res in result:
                in_json = json.dumps(res)
                to_send = in_json.encode('utf-8')
                out_stream = self.stream_anomaly_name if self.is_anomaly(res) else self.stream_out_name
                log.debug(f"STREAM {self.stream_name}: sending {to_send}")
                self.producer.send(out_stream, to_send)

            #delete the oldest record from cache
            updated_list = self.cache[group_by][1:]
            self.cache[group_by] = updated_list

    def make_timeseries_predictions(self):
        self.cache = Cache(self.stream_name)
        log.debug("STREAM: running 'make_timeseries_predictions'")
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"STREAM {self.stream_name} got error: requested predictor {self.predictor} is not exist")
            return
        ts_settings = self.get_ts_settings(predict_record)
        log.debug(f"STREAM {self.stream_name} TS_SETTINGS: {ts_settings}")
        self.target = ts_settings['to_predict'][0]
        self.window = ts_settings['window']
        self.gb = None if not ts_settings.get('group_by') else ts_settings.get('group_by')[0]
        self.dt = ts_settings['order_by'][0]

        while not self.stop_event.wait(0.5):
            try:
                msg_str = next(self.consumer)
                when_data = json.loads(msg_str.value)
                self.to_cache(when_data)
            except StopIteration:
                pass

        log.debug(f"STREAM {self.stream_name}: stopping...")
        self.producer.close()
        self.consumer.close()
        self.cache.delete()
        session.close()

    def make_prediction(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"STREAM {self.stream_name} got error: requested predictor {self.predictor} is not exist")
            return
        while not self.stop_event.wait(0.5):
            try:
                msg_str = next(self.consumer)
                when_data = json.loads(msg_str.value)
                result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
                log.debug(f"STREAM {self.stream_name}: got {result}")
                for res in result:
                    in_json = json.dumps(res)
                    to_send = in_json.encode('utf-8')
                    log.debug(f"STREAM {self.stream_name}: sending {to_send}")
                    out_stream = self.stream_anomaly_name if self.is_anomaly(res) else self.stream_out_name
                    self.producer.send(out_stream, to_send)
            except StopIteration:
                pass
        log.debug(f"STREAM {self.stream_name}: stopping...")
        self.producer.close()
        self.consumer.close()
        session.close()
