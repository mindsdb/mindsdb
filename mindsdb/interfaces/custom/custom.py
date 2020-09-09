import os
import shutil
import importlib
import json

import mindsdb_native
import pandas as pd

from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.utilities.fs import get_or_create_dir_struct

class CustomModels():
    def __init__(self, config):
        self.config = config
        self.dbw = DatabaseWrapper(self.config)
        _, _, _, self.storage_dir = get_or_create_dir_struct()
        self.model_cache = {}
        self.mindsdb_native = MindsdbNative(self.config)
        self.dbw = DatabaseWrapper(self.config)

    def _dir(self, name):
        return str(os.path.join(self.storage_dir, 'custom_model_' + name))

    def _internal_load(self, name):

        if name in self.model_cache:
            return self.model_cache[name]

        spec = importlib.util.spec_from_file_location(name, self._dir(name) + '/model.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        model = module.Model()
        if hasattr(model, 'setup'):
            model.setup()

        self.model_cache[name] = model

        return model

    def learn(self, name, from_data, to_predict, data_analysis, kwargs={}):
        data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])
        data_frame = data_source._df
        model = self._internal_load(name)

        data_analysis = self.mindsdb_native.get_analysis(ds_name)['data_analysis_v2']

        with open(os.path.join(self._dir(name), 'metadata.json'), 'w') as fp:
            json.dump({
                'name': name
                ,'data_analysis': data_analysis
                ,'predict': to_predict
            }, fp)

        model.fit(data_frame, to_predict, data_analysis, kwargs)

    def predict(self, name, when_data=None, from_data=None, kwargs={}):
        if from_data is not None:
            data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])
            data_frame = data_source._df
        elif when_data is not None:
            if isinstance(when_data, dict):
                for k in when_data: when_data[k] = [when_data[k]]
                data_frame = pd.DataFrame(when_data)
            else:
                data_frame = when_data

        model = self._internal_load(name)
        predictions = model.predict(data_frame, kwargs)

        pred_arr = []
        for i in range(len(predictions)):
            pred_arr.append({})
            pred_arr[-1] = {}
            for col in predictions.columns:
                pred_arr[-1][col] = {}
                pred_arr[-1][col]['predicted_value'] = predictions[col].iloc[i]

        print(pred_arr)
        return pred_arr

    def get_model_data(self, name):
        pass

    def get_models(self, status='any'):
        models = []
        for dir in os.listdir(self.storage_dir):
            if 'custom_model_' in dir:
                with open(os.path.join(self.storage_dir, dir, 'metadata.json'), 'r') as fp:
                    models.append(json.load(fp))

        return models

    def delete_model(self, name):
        shutil.rmtree(self._dir(name))
        self.dbw.unregister_predictor(name)

    def rename_model(self, name, new_name):
        shutil.move(self._dir(name), self._dir(new_name))

    def load_model(self, fpath, name):
        shutil.unpack_archive(fpath, self._dir(name), 'zip')
        with open(os.path.join(self._dir(name), 'metadata.json') , 'w') as fp:
            json.dump({
                'name': name
                ,'data_analysis': {
                    'empty': {
                        'typing': {
                            'data_subtype': 'Text'
                        }
                    }
                }
                ,'predict': 'Unknown'
            }, fp)
