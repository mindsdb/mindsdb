# Mindsdb native interface
from pathlib import Path
import json
import datetime
from dateutil.parser import parse as parse_datetime

import mindsdb_native
from mindsdb_native import F
from mindsdb.utilities.fs import create_directory
from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
from mindsdb.interfaces.native.learn_process import LearnProcess
from mindsdb.interfaces.database.database import DatabaseWrapper


class MindsdbNative():
    def __init__(self, config):
        self.config = config
        self.dbw = DatabaseWrapper(self.config)
        self.predictor_cache = {}
        self._load_counter = 0

    def _invalidate_cached_predictors(self):
        for predictor_name in list(predictor_cache.keys()):
            if (datetime.datetime.now() - predictor_cache[predictor_name]['created']).total_seconds() > 1200:
                del predictor_cache[predictor_name]

    def _setup_for_creation(self, name):
        if name in predictor_cache:
            del predictor_cache[name]
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
        if name not in predictor_cache:
            # @TODO Add some almost-OOM check and invalidate some of the older predictors if we are running OOM
            if F.get_model_data(name)['status'] == 'complete':
                predictor_cache[name] = {
                    'predictor': mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'}),
                    'created': datetime.datetime.now()
                }
                
        predictions = predictor_cache[name]['predictor'].predict(
            when_data=when_data,
            **kwargs
        )

        return predictions

    def analyse_dataset(self, ds):
        return F.analyse_dataset(ds)

    def get_model_data(self, name, native_view=False):
        model = F.get_model_data(name)
        if native_view:
            return model

        data_analysis = model['data_analysis_v2']
        for column in data_analysis['columns']:
            analysis = data_analysis.get(column)
            if isinstance(analysis, dict) and (len(analysis) == 0 or analysis.get('empty', {}).get('is_empty', False)):
                data_analysis[column]['typing'] = {
                    'data_subtype': DATA_SUBTYPES.INT
                }

        return model

    def get_models(self, status='any'):
        models = F.get_models()
        if status != 'any':
            models = [x for x in models if x['status'] == status]
        models = [x for x in models if x['status'] != 'training' or parse_datetime(x['created_at']) > parse_datetime(self.config['mindsdb_last_started_at'])]

        for i in range(len(models)):
            for k in ['train_end_at', 'updated_at', 'created_at']:
                if k in models[i] and models[i][k] is not None:
                    try:
                        models[i][k] = parse_datetime(str(models[i][k]).split('.')[0])
                    except Exception:
                        models[i][k] = parse_datetime(str(models[i][k]))
        return models

    def delete_model(self, name):
        F.delete_model(name)
        self.dbw.unregister_predictor(name)

    def rename_model(self, name, new_name):
        self.dbw.unregister_predictor(self.get_model_data(name))
        F.rename_model(name, new_name)
        self.dbw.register_predictors(self.get_model_data(new_name), setup=False)

    def load_model(self, fpath):
        F.import_model(model_archive_path=fpath)
        # @TODO How do we figure out the name here ?
        # dbw.register_predictors(...)

    def export_model(self, name):
        F.export_predictor(model_name=name)
