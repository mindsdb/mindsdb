import os
from threading import Event, Thread
from time import time

import pandas as pd
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterface
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.cache import Cache
from mindsdb.utilities.config import Config


class StreamController:
    def __init__(self, name, predictor, stream_in, stream_out, anomaly_stream=None, learning_stream=None, learning_threshold=100):
        self.name = name
        self.predictor = predictor

        self.stream_in = stream_in
        self.stream_out = stream_out
        self.anomaly_stream = anomaly_stream
        
        self.learning_stream = learning_stream
        self.learning_threshold = learning_threshold
        self.learning_data = []

        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.stop_event = Event()
        self.native_interface = ModelInterface()
        self.data_store = DataStore()
        self.config = Config()

        p = db.session.query(db.Predictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if p is None:
            raise Exception(f'Predictor {predictor} doesn\'t exist')

        self.target = p.learn_args['to_predict'] if isinstance(p.learn_args['to_predict'], str) else p.learn_args['to_predict'][0]

        ts_settings = p.learn_args['kwargs'].get('timeseries_settings', None)

        if ts_settings is None:
            self.thread = Thread(target=StreamController._make_predictions, args=(self,))
        else:
            self.ts_settings = ts_settings
            self.thread = Thread(target=StreamController._make_ts_predictions, args=(self,))

        self.thread.start()

    def _is_anomaly(self, res):
        for k in res:
            if k.endswith('_anomaly') and res[k] is not None:
                return True
        return False

    def _consider_learning(self):
        if self.learning_stream is not None:
            self.learning_data.extend(self.learning_stream.read())
            if len(self.learning_data) >= self.learning_threshold:
                p = db.session.query(db.Predictor).filter_by(company_id=self.company_id, name=self.predictor).first()
                ds_record = db.session.query(db.Datasource).filter_by(id=p.datasource_id).first()

                df = pd.DataFrame.from_records(self.learning_data)
                name = 'name_' + str(time()).replace('.', '_')
                path = os.path.join(self.config['paths']['datasources'], name)
                df.to_csv(path)

                from_data = {
                    'class': 'FileDS',
                    'args': [path],
                    'kwargs': {},
                }

                self.data_store.save_datasource(name=name, source_type='file', source=path, file_path=path, company_id=self.company_id)
                ds = self.data_store.get_datasource(name, self.company_id)

                self.model_interface.adjust(p.name, from_data, ds['id'], self.company_id)
                self.learning_data.clear()

    def _make_predictions(self):
        while not self.stop_event.wait(0.5):
            self._consider_learning()
            for when_data in self.stream_in.read():
                for res in self.native_interface.predict(self.predictor, 'dict', when_data=when_data):
                    if self.anomaly_stream is not None and self._is_anomaly(res):
                        self.anomaly_stream.write(res)
                    else:
                        self.stream_out.write(res)

    def _make_ts_predictions(self):
        window = self.ts_settings['window']

        order_by = self.ts_settings['order_by']
        order_by = [order_by] if isinstance(order_by, str) else order_by

        group_by = self.ts_settings.get('group_by', None)
        group_by = [group_by] if isinstance(group_by, str) else group_by

        cache = Cache(self.name)
        if group_by is None:

            if '' not in cache:
                cache[''] = []

            while not self.stop_event.wait(0.5):
                self._consider_learning()
                with cache:
                    for when_data in self.stream_in.read():
                        for ob in order_by:
                            if ob not in when_data:
                                raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                        records = cache['']
                        records.append(when_data)
                        cache[''] = records

                    if len(cache['']) >= window:
                        cache[''] = [*sorted(
                            cache[''],
                            # WARNING: assuming wd[ob] is numeric
                            key=lambda wd: tuple(wd[ob] for ob in order_by)
                        )]
                        res_list = self.native_interface.predict(self.predictor, 'dict', when_data=cache[''][-window:])
                        if self.anomaly_stream is not None and self._is_anomaly(res_list[-1]):
                            self.anomaly_stream.write(res_list[-1])
                        else:
                            self.stream_out.write(res_list[-1])
                        cache[''] = cache[''][1 - window:]
        else:

            while not self.stop_event.wait(0.5):
                self._consider_learning()
                for when_data in self.stream_in.read():
                    for ob in order_by:
                        if ob not in when_data:
                            raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                    for gb in group_by:
                        if gb not in when_data:
                            raise Exception(f'when_data doesn\'t contain group_by[{gb}]')

                    gb_value = tuple(when_data[gb] for gb in group_by)

                    # because cache doesn't work for tuples
                    # (raises Exception: tuple doesn't have "encode" attribute)
                    gb_value = str(gb_value)

                    with cache:
                        if gb_value not in cache:
                            cache[gb_value] = []

                        # do this because shelve-cache doesn't support
                        # in-place changing
                        records = cache[gb_value]
                        records.append(when_data)
                        cache[gb_value] = records

                with cache:
                    for gb_value in cache.keys():
                        if len(cache[gb_value]) >= window:
                            cache[gb_value] = [*sorted(
                                cache[gb_value],
                                # WARNING: assuming wd[ob] is numeric
                                key=lambda wd: tuple(wd[ob] for ob in order_by)
                            )]
                            res_list = self.native_interface.predict(self.predictor, 'dict', when_data=cache[gb_value][-window:])
                            if self.anomaly_stream is not None and self._is_anomaly(res_list[-1]):
                                self.anomaly_stream.write(res_list[-1])
                            else:
                                self.stream_out.write(res_list[-1])
                            cache[gb_value] = cache[gb_value][1 - window:]
