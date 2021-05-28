import os
from threading import Event
from mindsdb.utilities.log import log
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface


class StreamTypes:
    timeseries = "timeseries"


class BaseStream:
    def __init__(self):
        self.format_flag = 'dict'
        self.stop_event = Event()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.target = None
        self.window = None
        self.gb = None
        self.dt = None
        self.native_interface = NativeInterface()

    @staticmethod
    def get_ts_settings(predict_record):
        ts_settings = predict_record.learn_args['kwargs'].get('timeseries_settings', None)
        if ts_settings is None:
            raise Exception(f"Attempt to use {predict_record.name} predictor (not TS type) as timeseries predictor.")
        ts_settings['to_predict'] = predict_record.learn_args['to_predict']
        return ts_settings

    @staticmethod
    def is_anomaly(prediction):
        for key in prediction:
            if "anomaly" in key and prediction[key] is not None:
                return True
        return False

    def make_prediction_from_cache(self, group_by):
        with self.cache:
            if group_by in self.cache:
                if len(self.cache[group_by]) >= self.window:
                    self.predict_ts(group_by)
            else:
                log.debug(f"STREAM {self.stream_name}: creating empty cache for {group_by} group")
                self.cache[group_by] = []

    def to_cache(self, record):
        group_by = record.get(self.gb, 'no_group_by')
        self.make_prediction_from_cache(group_by)
        self.handle_record(group_by, record)
        self.make_prediction_from_cache(group_by)

    def handle_record(self, group_by, record):
        log.debug(f"STREAM {self.stream_name}: handling cache {group_by} and {record} record.")
        with self.cache:
            records = self.cache[group_by]
            log.debug(f"STREAM: read {records} from cache.")
            records.append(record)
            records = self.sort_cache(records)
            self.cache[group_by] = records
        log.debug(f"STREAM {self.stream_name}: finish updating {group_by}")


    def sort_cache(self, cache):
        return sorted(cache, key=lambda x: x[self.dt])
