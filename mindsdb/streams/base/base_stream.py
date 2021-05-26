import os
from threading import Event, Thread

from mindsdb.interfaces.model.model_interface import ModelInterface
from collections import defaultdict
import mindsdb.interfaces.storage.db as db



class BaseStream:
    def __init__(self, name, predictor):
        self.name = name
        self.predictor = predictor

        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)

        self.stop_event = Event()

        self.native_interface = ModelInterface()

        p = db.session.query(db.Predictor).filter_by(company_id=self.company_id, name=self.predictor).first()

        if p is None:
            raise Exception(f'Predictor {predictor} doesn\'t exist')

        self.target = p.learn_args['to_predict']

        ts_settings = p.learn_args['kwargs'].get('timeseries_settings', None)

        if ts_settings is None:
            self.thread = Thread(target=BaseStream._make_predictions, args=(self,))
        else:
            self.ts_settings = ts_settings
            self.thread = Thread(target=BaseStream._make_ts_predictions, args=(self,))

        self.thread.start()

    def _read_from_in_stream(self):
        raise NotImplementedError

    def _del_from_in_stream(self, k):
        raise NotImplementedError

    def _write_to_out_stream(self, dct):
        raise NotImplementedError

    def write_to_in_stream(self, dct):
        raise NotImplementedError

    def _make_predictions(self):
        while not self.stop_event.wait(0.5):
            for k, when_data in self._read_from_in_stream():
                for res in self.native_interface.predict(self.predictor, 'dict', when_data=when_data):
                    self._write_to_out_stream(res)
                self._del_from_in_stream(k)

    def _make_ts_predictions(self):
        window = self.ts_settings['window']

        order_by = self.ts_settings['order_by']
        order_by = [order_by] if isinstance(order_by, str) else order_by

        group_by = self.ts_settings['group_by']
        group_by = [group_by] if isinstance(group_by, str) else group_by

        gb_dict = defaultdict(list)

        while not self.stop_event.wait(0.5):
            for k, when_data in self._read_from_in_stream():
                gb_value = tuple(when_data.get(gb, None) for gb in group_by)
                gb_hash = str(hash(gb_value))
                gb_dict[gb_hash].append(when_data)
                self._del_from_in_stream(k)

            for gb_hash, when_data_list in gb_dict.items():
                gb_dict[gb_hash] = sorted(
                    gb_dict[gb_hash],
                    key=lambda wd: (float(wd[ob]) for ob in order_by)
                )

                if len(when_data_list) >= window:
                    for res in self.native_interface.predict(self.predictor, 'dict', when_data=when_data_list[-window:]):
                        self._write_to_out_stream(res)
                    gb_dict[gb_hash] = when_data_list[1 - window:]

            import time
            time.sleep(2)

            print(gb_dict)
