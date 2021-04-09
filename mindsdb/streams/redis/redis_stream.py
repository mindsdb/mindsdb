import os
import json
from threading import Thread, Event
import walrus
from mindsdb.utilities.log import log
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface


class RedisStream(Thread):
    def __init__(self, name, host, port, database, stream_in, stream_out, predictor, _type):
        self.name = name
        self.host = host
        self.port = port
        self.db = database
        self.predictor = predictor
        self.client = self._get_client()
        self.stream_in_name = stream_in
        self.stream_out_name = stream_out
        self.stream_in = self.client.Stream(stream_in)
        self.stream_out = self.client.Stream(stream_out)
        self._type = _type
        self.native_interface = NativeInterface()
        self.format_flag = 'explain'

        self.stop_event = Event()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        if self._type == 'timeseries':
            super().__init__(target=RedisStream.make_timeseries_predictions, args=(self,))
        else:
            super().__init__(target=RedisStream.make_predictions, args=(self,))

    def _get_client(self):
        return walrus.Database(host=self.host, port=self.port, db=self.db)

    def _get_target(self):
        pass

    def _get_window_size(self):
        pass

    def _get_gb(self):
        pass

    def _get_dt(self):
        pass

    def predict(self, stream_in, stream_out, timeseries_mode=False):
        predict_info = stream_in.read(block=0)
        when_list = []
        for record in predict_info:
            record_id = record[0]
            raw_when_data = record[1]
            when_data = self.decode(raw_when_data)
            if timeseries_mode:
                if self.target not in when_data:
                    when_data['make_predictions'] = False
                else:
                    when_data['make_predictions'] = True
                when_list.append(when_data)
            else:
                result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
                log.error(f"STREAM: got {result}")
                for res in result:
                    in_json = json.dumps(res)
                    stream_out.add({"prediction": in_json})
                stream_in.delete(record_id)

        if timeseries_mode:
            result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_list)
            log.error(f"TIMESERIES STREAM: got {result}")
            for res in result:
                in_json = json.dumps(res)
                stream_out.add({"prediction": in_json})
            stream_in.trim(len(stream_in) - 1, approximate=False)


    def make_prediction_from_cache(self, cache):
        if len(cache) >= self.window:
            self.predict(cache, self.stream_out, timeseries_mode=True)

    def make_timeseries_predictions(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return
        self.cache = self.client.Stream(f"{self._name}_cache")
        self.target = self._get_target()
        self.window = self._get_window_size()
        self.gb = self._get_gb()
        self.dt = self._get_dt()

        while not self.stop_event.wait(0.5):
            # block==0 is a blocking mode
            predict_info = self.stream_in.read(block=0)
            for record in predict_info:
                record_id = record[0]
                raw_when_data = record[1]
                when_data = self.decode(raw_when_data)
                self.to_cache(when_data)
                self.stream_in.delete(record_id)
        session.close()

    def to_cache(self, record):
        gb_val = record[self.gb]
        cache = self.client.Stream(f"{self.name}.cache.{gb_val}")
        self.make_prediction_from_cache(cache)
        self.handle_record(cache, record)
        self.make_prediction_from_cache(cache)

    def handle_record(self, cache, record):
        records = cache.read(block=0)
        records = [self.decode(x) for x in records]
        records.append(record)
        records = self.sort_cache(records)
        cache.trim(0, approximate=True)
        for rec in records:
            cache.add(rec)

    def sort_cache(self, cache):
        return sorted(cache, key=lambda x: x[self.dt])

    def make_predictions(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return

        while not self.stop_event.wait(0.5):
            # block==0 is a blocking mode
            predict_info = self.stream_in.read(block=0)
            for record in predict_info:
                record_id = record[0]
                raw_when_data = record[1]
                when_data = self.decode(raw_when_data)

                result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
                log.error(f"STREAM: got {result}")
                for res in result:
                    in_json = json.dumps(res)
                    self.stream_out.add({"prediction": in_json})
                self.stream_in.delete(record_id)

        session.close()

    def decode(self, redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded
