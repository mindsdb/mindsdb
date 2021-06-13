import os
from threading import Event, Thread
from collections import defaultdict

import pandas as pd
import mindsdb_datasources
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterface
import mindsdb.interfaces.storage.db as db


class StreamController:
    def __init__(self, name, predictor, stream_in, stream_out, learning_stream=None, learning_threshold=100):
        self.name = name
        self.predictor = predictor

        self.stream_in = stream_in
        self.stream_out = stream_out
        
        self.learning_stream = learning_stream
        self.learning_threshold = learning_threshold
        self.learning_data = []

        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.stop_event = Event()
        self.native_interface = ModelInterface()
        self.data_store = DataStore()

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

    def _consider_learning(self):
        if self.learning_stream is not None:
            self.learning_data.extend(self.learning_stream.read())
            if len(self.learning_data) >= self.learning_threshold:
                # 1. Create a new datasource from self.learning_data
                # 2. Add it to db.Predictor.additional_datasources
                # 3. Call self.model_interface.adjust(...)
                self.learning_data.clear()

    def _make_predictions(self):
        while not self.stop_event.wait(0.5):
            self._consider_learning()
            for when_data in self.stream_in.read():
                for res in self.native_interface.predict(self.predictor, 'dict', when_data=when_data):
                    self.stream_out.write(res)

    def _make_ts_predictions(self):
        window = self.ts_settings['window']

        order_by = self.ts_settings['order_by']
        order_by = [order_by] if isinstance(order_by, str) else order_by

        group_by = self.ts_settings['group_by']
        group_by = [group_by] if isinstance(group_by, str) else group_by

        if group_by is None:
            cache = []

            while not self.stop_event.wait(0.5):
                self._consider_learning()
                for when_data in self.stream_in.read():
                    for ob in order_by:
                        if ob not in when_data:
                            raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                    cache.append(when_data)
                
                if len(cache) >= window:
                    cache = [*sorted(
                        cache,
                        # WARNING: assuming wd[ob] is numeric
                        key=lambda wd: tuple(wd[ob] for ob in order_by)
                    )]
                    res_list = self.native_interface.predict(self.predictor, 'dict', when_data=cache[-window:])
                    self.stream_out.write(res_list[-1])
                    cache = cache[1 - window:]
        else:
            gb_cache = defaultdict(list)

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
                    gb_cache[gb_value].append(when_data)

                for gb_value in gb_cache.keys():
                    if len(gb_cache[gb_value]) >= window:
                        gb_cache[gb_value] = [*sorted(
                            gb_cache[gb_value],
                            # WARNING: assuming wd[ob] is numeric
                            key=lambda wd: tuple(wd[ob] for ob in order_by)
                        )]
                        res_list = self.native_interface.predict(self.predictor, 'dict', when_data=gb_cache[gb_value][-window:])
                        self.stream_out.write(res_list[-1])
                        gb_cache[gb_value] = gb_cache[gb_value][1 - window:]
