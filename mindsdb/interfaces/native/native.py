# Mindsdb native interface
from pathlib import Path
import os
import datetime
from dateutil.parser import parse as parse_datetime
import psutil

import mindsdb_native
from mindsdb_native import F
from mindsdb.utilities.fs import create_directory
from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
from mindsdb.interfaces.native.learn_process import LearnProcess
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.interfaces.storage.fs import FsSotre


class NativeInterface():
    def __init__(self):
        self.config = Config()
        self.fs_store = FsSotre()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.dbw = DatabaseWrapper()
        self.predictor_cache = {}

    def _invalidate_cached_predictors(self):
        # @TODO: Cache will become stale if the respective NativeInterface is not invoked yet a bunch of predictors remained cached, no matter where we invoke it. In practice shouldn't be a big issue though
        for predictor_name in list(self.predictor_cache.keys()):
            if (datetime.datetime.now() - self.predictor_cache[predictor_name]['created']).total_seconds() > 1200:
                del self.predictor_cache[predictor_name]

    def _setup_for_creation(self, name):
        if name in self.predictor_cache:
            del self.predictor_cache[name]
        # Here for no particular reason, because we want to run this sometimes but not too often
        self._invalidate_cached_predictors()

        predictor_dir = Path(self.config.paths['predictors']).joinpath(name)
        create_directory(predictor_dir)
        predictor_record = Predictor(company_id=self.company_id, name=name, is_custom=False)

        session.add(predictor_record)
        session.commit()

    def create(self, name):
        self._setup_for_creation(name)
        predictor = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})
        return predictor

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        self._setup_for_creation(name)

        p = LearnProcess(name, from_data, to_predict, kwargs, datasource_id)
        p.start()
        if join_learn_process is True:
            p.join()
            if p.exitcode != 0:
                raise Exception('Learning process failed !')

    def predict(self, name, when_data=None, kwargs={}):
        if name not in self.predictor_cache:
            # Clear the cache entirely if we have less than 1.2 GB left
            if psutil.virtual_memory().available < 1.2 * pow(10,9):
                self.predictor_cache = {}

            predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
            if predictor_record.data['status'] == 'complete':
                self.fs_store.get(name, f'predictor_{self.company_id}_{predictor_record.id}', self.config['paths']['predictors'])
                self.predictor_cache[name] = {
                    'predictor': mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'}),
                    'created': datetime.datetime.now()
                }

        predictions = self.predictor_cache[name]['predictor'].predict(
            when_data=when_data,
            **kwargs
        )

        return predictions

    # @TODO Move somewhere else to avoid circular import issues in the future
    def analyse_dataset(self, ds):
        return F.analyse_dataset(ds)

    def get_model_data(self, name, db_fix=True):
        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
        model = predictor_record.data
        if model is None or model['status'] == 'training':
            try:
                self.fs_store.get(name, f'predictor_{self.company_id}_{predictor_record.id}', self.config['paths']['predictors'])
                new_model_data = mindsdb_native.F.get_model_data(name)
            except Exception:
                pass
            if predictor_record.data is None or len(new_model_data) > len(predictor_record.data):
                predictor_record.data = new_model_data
                model = new_model_data
                session.commit()

        # Make some corrections for databases not to break when dealing with empty columns
        if db_fix:
            data_analysis = model['data_analysis_v2']
            for column in model['columns']:
                analysis = data_analysis.get(column)
                if isinstance(analysis, dict) and (len(analysis) == 0 or analysis.get('empty', {}).get('is_empty', False)):
                    data_analysis[column]['typing'] = {
                        'data_subtype': DATA_SUBTYPES.INT
                    }

        return model

    def get_models(self):
        models = []
        predictor_names = [
            x.name for x in Predictor.query.filter_by(company_id=self.company_id, is_custom=False)
        ]
        for model_name in predictor_names:
            try:
                model_data = self.get_model_data(model_name, db_fix=False)
                if model_data['status'] == 'training' and parse_datetime(model_data['created_at']) < parse_datetime(self.config['mindsdb_last_started_at']):
                    continue

                reduced_model_data = {}

                for k in ['name', 'version', 'is_active', 'predict', 'status', 'current_phase', 'accuracy', 'data_source']:
                    reduced_model_data[k] = model_data.get(k, None)

                for k in ['train_end_at', 'updated_at', 'created_at']:
                    reduced_model_data[k] = model_data.get(k, None)
                    if reduced_model_data[k] is not None:
                        try:
                            reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]).split('.')[0])
                        except Exception as e:
                            # @TODO Does this ever happen
                            print(f'Date parsing exception while parsing: {k} in get_models: ', e)
                            reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]))

                models.append(reduced_model_data)
            except Exception as e:
                print(f"Can't list data for model: '{model_name}' when calling `get_models(), error: {e}`")

        return models

    def delete_model(self, name):
        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
        id = predictor_record.id
        session.delete(predictor_record)
        session.commit()
        F.delete_model(name)
        self.dbw.unregister_predictor(name)
        self.fs_store.delete(f'predictor_{self.company_id}_{id}')
