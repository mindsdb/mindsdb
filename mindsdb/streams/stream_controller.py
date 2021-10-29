import os
import traceback
from threading import Event, Thread
from time import time, sleep
from tempfile import NamedTemporaryFile

import requests
import pandas as pd
from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterfaceWrapper, ModelInterface
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.cache import Cache
from mindsdb.utilities.config import Config


class StreamController:
    def __init__(self, name, predictor, stream_in, stream_out, anomaly_stream=None):
        self.name = name
        self.predictor = predictor

        self.stream_in = stream_in
        self.stream_out = stream_out
        self.anomaly_stream = anomaly_stream

        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.stop_event = Event()
        self.model_interface = ModelInterfaceWrapper(ModelInterface())
        self.data_store = DataStore()
        self.config = Config()

        p = db.session.query(db.Predictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if p is None:
            raise Exception(f'Predictor {predictor} doesn\'t exist')

        self.target = p.to_predict[0]

        ts_settings = p.learn_args.get('timeseries_settings', {})
        if isinstance(ts_settings, dict) is False or ts_settings.get('is_timeseries', False) is False:
            ts_settings = None

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

    def _make_predictions(self):
        while not self.stop_event.wait(0.5):
            for when_data in self.stream_in.read():
                preds = self.model_interface.predict(self.predictor, when_data, 'dict')
                for res in preds:
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

        while not self.stop_event.wait(0.5):
            for when_data in self.stream_in.read():
                for ob in order_by:
                    if ob not in when_data:
                        raise Exception(f'when_data doesn\'t contain order_by[{ob}]')

                for gb in group_by:
                    if gb not in when_data:
                        raise Exception(f'when_data doesn\'t contain group_by[{gb}]')

                gb_value = tuple(when_data[gb] for gb in group_by) if group_by is not None else ''

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

                        while len(cache[gb_value]) >= window:
                            res_list = self.model_interface.predict(self.predictor, cache[gb_value][:window], 'dict')
                            if self.anomaly_stream is not None and self._is_anomaly(res_list[-1]):
                                self.anomaly_stream.write(res_list[-1])
                            else:
                                self.stream_out.write(res_list[-1])
                            cache[gb_value] = cache[gb_value][1:]


class StreamLearningController:
    def __init__(self, name, predictor, learning_params, stream_in, learning_threshold, stream_out, integration):
        self.name = name
        self.predictor = predictor
        self.learning_params = learning_params
        self.learning_threshold = learning_threshold
        self.learning_data = []

        self.config = Config()
        self.integration = integration
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.mindsdb_url = f"http://{self.config['api']['http']['host']}:{self.config['api']['http']['port']}"
        self.mindsdb_api_root = self.mindsdb_url + "/api"
        self.stream_in = stream_in
        self.stream_out = stream_out
        self.training_ds_name = self.predictor + "_training_ds"


        # for consistency only
        self.stop_event = Event()

        self.thread = Thread(target=StreamLearningController._learn_model, args=(self,))

        self.thread.start()

    def _upload_ds(self, df):
        with NamedTemporaryFile(mode='w+', newline='', delete=False) as f:
            df.to_csv(f, index=False)
            f.flush()
            url = f'{self.mindsdb_api_root}/datasources/{self.training_ds_name}'
            data = {
                "source_type": (None, 'file'),
                "file": (f.name, f, 'text/csv'),
                "source": (None, f.name.split('/')[-1]),
                "name": (None, self.training_ds_name)
            }
            res = requests.put(url, files=data)
            res.raise_for_status()

    def _collect_training_data(self):
        threshold = time() + self.learning_threshold
        while time() < threshold:
            self.learning_data.extend(self.stream_in.read())
            sleep(0.2)
        return pd.DataFrame.from_records(self.learning_data)

    def _learn_model(self):
        try:
            p = db.session.query(db.Predictor).filter_by(
                    company_id=self.company_id, name=self.predictor).first()
            if p is not None:
                predictor_name = f"TMP_{self.predictor}_TMP"
            else:
                predictor_name = self.predictor

            df = self._collect_training_data()
            self._upload_ds(df)
            self.learning_params['data_source_name'] = self.training_ds_name
            if 'kwargs' not in self.learning_params:
                self.learning_params['kwargs'] = {}
            self.learning_params['kwargs']['join_learn_process'] = True
            url = f'{self.mindsdb_api_root}/predictors/{predictor_name}'
            res = requests.put(url, json=self.learning_params)
            res.raise_for_status()

            if p is not None:
                delete_url = f'{self.mindsdb_api_root}/predictors/{self.predictor}'
                rename_url = f'{self.mindsdb_api_root}/predictors/{predictor_name}/rename?new_name={self.predictor}'
                res = requests.delete(delete_url)
                res.raise_for_status()
                res = requests.get(rename_url)
                res.raise_for_status()
            msg = {"action": "training", "predictor": self.predictor,
                    "status": "success", "details": ""}
        except Exception as e:
            msg = {"action": "training", "predictor": self.predictor,
                    "status": "error", "details": traceback.format_exc()}
        self.stream_out.write(msg)

        # Need to delete its own record from db to mark is at outdated
        # for integration, which delete it from 'active threads' after that
        db.session.query(db.Stream).filter_by(
                company_id=self.company_id, integration=self.integration, name=self.name).delete()
        db.session.commit()
