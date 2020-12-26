# Mindsdb native interface
from pathlib import Path
import json

from dateutil.parser import parse as parse_datetime

import mindsdb_native
from mindsdb_native import F
from mindsdb.utilities.fs import create_directory
from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
from mindsdb.interfaces.native.predictor_process import PredictorProcess
from mindsdb.interfaces.state.state import State
from mindsdb.interfaces.state.config import Config

class MindsdbNative():
    def __init__(self, config):
        self.config = Config(config)
        self.state = State(self.config)

    def _setup_for_creation(self, name):
            predictor_dir = Path(self.config.paths['predictors']).joinpath(name)
            create_directory(predictor_dir)

    def create(self, name):
        # Just used for getting the report uuid, don't bother registering this
        self._setup_for_creation(name)
        predictor = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})
        return predictor

    def learn(self, name, from_data, to_predict, kwargs={}):
        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        self._setup_for_creation(name)

        p = PredictorProcess(name, from_data, to_predict, kwargs, 'learn', self.config._config)
        p.start()
        if join_learn_process is True:
            p.join()
            if p.exitcode != 0:
                raise Exception('Learning process failed !')

    def predict(self, name, when_data=None, kwargs={}):
        # @TODO Separate into two paths, one for "normal" predictions and one for "real time" predictions. Use the multiprocessing code commented out bellow for normal (once we figure out how to return the prediction object... else use the inline code but with the "real time" predict functionality of mindsdb_native taht will be implemented later)
        '''
        from_data = when if when is not None else when_data
        p = PredictorProcess(name, from_data, to_predict=None, kwargs=kwargs, config=self.config.as_dict(), 'predict')
        p.start()
        predictions = p.join()
        '''
        self.state.load_predictor(name)
        mdb = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})

        predictions = mdb.predict(
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
            if len(data_analysis[column]) == 0 or data_analysis[column].get('empty', {}).get('is_empty', False):
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
        self.state.delete_predictor(name)
        F.delete_model(name)

    def rename_model(self, name, new_name):
        F.rename_model(name, new_name)

    def load_model(self, fpath):
        # self.state.make_predictor(name, None, to_predict) <--- fix
        F.import_model(model_archive_path=fpath)

    def export_model(self, name):
        F.export_predictor(model_name=name)
