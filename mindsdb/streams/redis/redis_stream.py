import json
from threading import Thread, Event
import walrus

from mindsdb.utilities.log import log
from mindsdb.utilities.cache import Cache
from mindsdb.streams.base.base_stream import StreamTypes, BaseStream
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface


class RedisStream(Thread, BaseStream):
    def __init__(self, name, connection_info, advanced_info, stream_in, stream_out, stream_anomaly, predictor, _type):
        self.stream_name = name
        self.connection_info = connection_info
        self.connection_info.update(advanced_info)
        self.predictor = predictor
        self.client = self._get_client()
        self.stream_in_name = stream_in
        self.stream_out_name = stream_out
        self.stream_anomaly_name = stream_anomaly
        self.stream_in = self.client.Stream(stream_in)
        self.stream_out = self.client.Stream(stream_out)
        self.stream_anomaly = self.client.Stream(stream_anomaly)
        self._type = _type

        BaseStream.__init__(self)
        if self._type.lower() == StreamTypes.timeseries:
            super().__init__(target=RedisStream.make_timeseries_predictions, args=(self,))
        else:
            super().__init__(target=RedisStream.make_predictions, args=(self,))
            self.cache = None

    def _get_client(self):
        return walrus.Database(**self.connection_info)

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
                stream_out = self.stream_anomaly if self.is_anomaly(res) else self.stream_out
                stream_out.add({'prediction': in_json})

            #delete the oldest record from cache
            updated_list = self.cache[group_by][1:]
            self.cache[group_by] = updated_list

    def predict(self, stream_in):
        predict_info = stream_in.read()
        for record in predict_info:
            record_id = record[0]
            raw_when_data = record[1]
            when_data = self.decode(raw_when_data)
            result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
            log.debug(f"STREAM {self.stream_name}: got {result}")
            for res in result:
                in_json = json.dumps(res)
                stream_out = self.stream_anomaly if self.is_anomaly(res) else self.stream_out
                stream_out.add({'prediction': in_json})
            stream_in.delete(record_id)

    def make_predictions(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"STREAM {self.stream_name} got error: requested predictor {self.predictor} is not exist")
            return

        while not self.stop_event.wait(0.5):
            self.predict(self.stream_in)

        session.close()
        log.debug(f"STREAM {self.stream_name}: stopping...")

    def make_timeseries_predictions(self):
        self.cache = Cache(self.stream_name)
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
            predict_info = self.stream_in.read()
            for record in predict_info:
                record_id = record[0]
                raw_when_data = record[1]
                when_data = self.decode(raw_when_data)
                log.debug(f"STREAM {self.stream_name}: next record have read from {self.stream_in.key}: {when_data}")
                self.to_cache(when_data)
                self.stream_in.delete(record_id)
        self.cache.delete()
        session.close()

    def decode(self, redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded
