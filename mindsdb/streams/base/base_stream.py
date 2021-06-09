import os
from threading import Event, Thread

from mindsdb.interfaces.model.model_interface import ModelInterface
from collections import defaultdict
import mindsdb.interfaces.storage.db as db


class BaseStream:
    def __init__(self, name, predictor, learn_threshold=100):
        self.name = name
        self.predictor = predictor
        self.learn_threshold = learn_threshold
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.stop_event = Event()
        self.native_interface = ModelInterface()

        p = db.session.query(db.Predictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if p is None:
            raise Exception(f'Predictor {predictor} doesn\'t exist')

        self.target = p.learn_args['to_predict'] if isinstance(p.learn_args['to_predict'], str) else p.learn_args['to_predict'][0]

        self.ts_settings = p.learn_args['kwargs'].get('timeseries_settings', None)
        if self.ts_settings is None:
            self.thread = Thread(target=BaseStream._make_predictions, args=(self,))
        else:
            self.thread = Thread(target=BaseStream._make_ts_predictions, args=(self,))
        self.thread.start()

    def _read_from_learning_stream(self):
        raise NotImplementedError

    def _get_learning_stream_size(self):
        raise NotImplementedError

    def _read_from_in_stream(self):
        raise NotImplementedError

    def _write_to_out_stream(self, dct):
        raise NotImplementedError

    def _consider_adjusting_model(self):
        if self._get_learning_stream_size() >= self.learn_threshold:
            when_data = list(self._read_from_learning_stream())
            print('adjusting model with {} rows'.format(len(when_data)))
            self.native_interface.adjust(self.predictor, 'dict', when_data=when_data)

    def _make_predictions(self):
        while not self.stop_event.wait(0.5):
            try:
                self._consider_adjusting_model()
            except NotImplementedError:
                pass
            for when_data in self._read_from_in_stream():
                for res in self.native_interface.predict(self.predictor, 'dict', when_data=when_data):
                    self._write_to_out_stream(res)

    def _make_ts_predictions(self):
        window = self.ts_settings['window']

        order_by = self.ts_settings['order_by']
        order_by = [order_by] if isinstance(order_by, str) else order_by

        group_by = self.ts_settings['group_by']
        group_by = [group_by] if isinstance(group_by, str) else group_by

        if group_by is None:
            cache = []

            while not self.stop_event.wait(0.5):
                try:
                    self._consider_adjusting_model()
                except NotImplementedError:
                    pass
                for when_data in self._read_from_in_stream():
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
                    self._write_to_out_stream(res_list[-1])
                    cache = cache[1 - window:]
        else:
            gb_cache = defaultdict(list)

            while not self.stop_event.wait(0.5):
                try:
                    self._consider_adjusting_model()
                except NotImplementedError:
                    pass
                for when_data in self._read_from_in_stream():
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
                        self._write_to_out_stream(res_list[-1])
                        gb_cache[gb_value] = gb_cache[gb_value][1 - window:]
