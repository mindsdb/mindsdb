# Mindsdb native interface
from pathlib import Path
import json

from dateutil.parser import parse as parse_datetime

import mindsdb_native
from mindsdb_native import F
from mindsdb.utilities.fs import create_directory
from mindsdb.interfaces.native.predictor_process import PredictorProcess
from mindsdb.interfaces.database.database import DatabaseWrapper


class MindsdbNative():
    def __init__(self, config):
        self.config = config
        self.dbw = DatabaseWrapper(self.config)

    def learn(self, name, from_data, to_predict, kwargs={}):
        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        predictor_dir = Path(self.config.paths['predictors']).joinpath(name)
        create_directory(predictor_dir)
        versions_file_path = predictor_dir.joinpath('versions.json')
        with open(str(versions_file_path), 'wt') as f:
            json.dump(self.config.versions, f, indent=4, sort_keys=True)

        p = PredictorProcess(name, from_data, to_predict, kwargs, self.config.get_all(), 'learn')
        p.start()
        if join_learn_process is True:
            p.join()
            if p.exitcode != 0:
                raise Exception('Learning process failed !')

    def predict(self, name, when_data=None, kwargs={}):
        # @TODO Separate into two paths, one for "normal" predictions and one for "real time" predictions. Use the multiprocessing code commented out bellow for normal (once we figure out how to return the prediction object... else use the inline code but with the "real time" predict functionality of mindsdb_native taht will be implemented later)
        '''
        from_data = when if when is not None else when_data
        p = PredictorProcess(name, from_data, to_predict=None, kwargs=kwargs, config=self.config.get_all(), 'predict')
        p.start()
        predictions = p.join()
        '''
        mdb = mindsdb_native.Predictor(name=name)

        predictions = mdb.predict(
            when_data=when_data,
            **kwargs
        )

        return predictions

    def analyse_dataset(self, ds):
        return F.analyse_dataset(ds)

    def get_model_data(self, name):
        return F.get_model_data(name)

    def get_models(self, status='any'):
        models = F.get_models()
        if status != 'any':
            models = [x for x in models if x['status'] == status]

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
