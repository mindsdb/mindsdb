# Mindsdb native interface
from pathlib import Path
import json
import datetime
from dateutil.parser import parse as parse_datetime
import psutil

import mindsdb_native
from mindsdb_native import F
from mindsdb.utilities.fs import create_directory
from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
from mindsdb.interfaces.native.learn_process import LearnProcess
from mindsdb.interfaces.database.database import DatabaseWrapper


class NativeInterface():
    def __init__(self, config):
        self.config = config
        self.dbw = DatabaseWrapper(self.config)
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
        versions_file_path = predictor_dir.joinpath('versions.json')
        with open(str(versions_file_path), 'wt') as f:
            json.dump(self.config.versions, f, indent=4, sort_keys=True)

    def create(self, name):
        self._setup_for_creation(name)
        predictor = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})
        return predictor

    def learn(self, name, from_data, to_predict, kwargs={}):
        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        self._setup_for_creation(name)

        p = LearnProcess(name, from_data, to_predict, kwargs, self.config.get_all())
        p.start()
        if join_learn_process is True:
            p.join()
            if p.exitcode != 0:
                raise Exception('Learning process failed !')

    def predict(self, name, when_data=None, kwargs={}):
        if name not in self.predictor_cache:
            # Clear the cache entirely if we have less than .12 GB left
            if psutil.virtual_memory().available < 1.2 * pow(10,9):
                self.predictor_cache = {}

            if F.get_model_data(name)['status'] == 'complete':
                self.predictor_cache[name] = {
                    'predictor': mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'}),
                    'created': datetime.datetime.now()
                }

        predictions = self.predictor_cache[name]['predictor'].predict(
            when_data=when_data,
            **kwargs
        )

        return predictions

    def analyse_dataset(self, ds):
        return F.analyse_dataset(ds)

    def get_model_data(self, name, db_fix=True):
        model = F.get_model_data(name)

        # Make some corrections for databases not to break when dealing with empty columns
        if db_fix:
            data_analysis = model['data_analysis_v2']
            for column in data_analysis['columns']:
                analysis = data_analysis.get(column)
                if isinstance(analysis, dict) and (len(analysis) == 0 or analysis.get('empty', {}).get('is_empty', False)):
                    data_analysis[column]['typing'] = {
                        'data_subtype': DATA_SUBTYPES.INT
                    }

        return model

    def get_models(self):
        models = []
        predictors = [
            x for x in Path(self.config.paths['predictors']).iterdir() if
                x.is_dir()
                and x.joinpath('light_model_metadata.pickle').is_file()
                and x.joinpath('heavy_model_metadata.pickle').is_file()
        ]
        for p in predictors:
            model_name = p.name
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
        F.delete_model(name)
        self.dbw.unregister_predictor(name)

    def rename_model(self, name, new_name):
        self.dbw.unregister_predictor(self.get_model_data(name))
        F.rename_model(name, new_name)
        self.dbw.register_predictors(self.get_model_data(new_name))

    def load_model(self, fpath):
        name = F.import_model(model_archive_path=fpath)
        self.dbw.register_predictors(self.get_model_data(name), setup=False)

    def export_model(self, name):
        F.export_predictor(model_name=name)
