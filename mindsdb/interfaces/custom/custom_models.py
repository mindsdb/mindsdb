import os
import shutil
import importlib
import json
import sys

import mindsdb_native
import pandas as pd

from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.native.native import NativeInterface

class CustomModels():
    def __init__(self, config):
        self.config = config
        self.dbw = DatabaseWrapper(self.config)
        self.storage_dir = os.path.join(config['storage_dir'], 'misc')
        os.makedirs(self.storage_dir, exist_ok=True)
        self.model_cache = {}
        self.mindsdb_native = NativeInterface(self.config)
        self.dbw = DatabaseWrapper(self.config)

    def _dir(self, name):
        return str(os.path.join(self.storage_dir, 'custom_model_' + name))

    def _internal_load(self, name):

        # Caching (2 lines bellow), currently disabled due to multiprocessing cache invalidation issues
        #if name in self.model_cache:
        #    return self.model_cache[name]

        # "Proper" model loading (3 lines bellow), currently disabled due to pickling issues
        #spec = importlib.util.spec_from_file_location(name, self._dir(name) + '/model.py')
        #module = importlib.util.module_from_spec(spec)
        #spec.loader.exec_module(module)

        sys.path.insert(0, self._dir(name))
        module = __import__(name)

        try:
            model = module.Model.load(os.path.join(self._dir(name), 'model.pickle'))
        except Exception as e:
            model = module.Model()
            model.initialize_column_types()
            if hasattr(model, 'setup'):
                model.setup()

        self.model_cache[name] = model

        return model

    def learn(self, name, from_data, to_predict, kwargs={}):
        model_data = self.get_model_data(name)
        model_data['status'] = 'training'
        self.save_model_data(name, model_data)

        to_predict = to_predict if isinstance(to_predict,list) else [to_predict]
        data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])
        data_frame = data_source.df
        model = self._internal_load(name)
        model.to_predict = to_predict

        model_data = self.get_model_data(name)
        model_data['predict'] = model.to_predict
        self.save_model_data(name, model_data)

        data_analysis = self.mindsdb_native.analyse_dataset(data_source)['data_analysis_v2']

        model_data = self.get_model_data(name)
        model_data['data_analysis_v2'] = data_analysis
        self.save_model_data(name, model_data)

        model.fit(data_frame, to_predict, data_analysis, kwargs)

        model.save(os.path.join(self._dir(name), 'model.pickle'))
        self.model_cache[name] = model

        model_data = self.get_model_data(name)
        model_data['status'] = 'completed'
        self.save_model_data(name, model_data)

        self.dbw.unregister_predictor(name)
        self.dbw.register_predictors([self.get_model_data(name)])

    def predict(self, name, when_data=None, from_data=None, kwargs=None):
        if kwargs is None:
            kwargs = {}
        if from_data is not None:
            if isinstance(from_data, dict):
                data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'],
                                                                          **from_data['kwargs'])
            # assume that particular instance of any DataSource class is provided
            else:
                data_source = from_data
            data_frame = data_source.df
        elif when_data is not None:
            if isinstance(when_data, dict):
                for k in when_data:
                    when_data[k] = [when_data[k]]
                data_frame = pd.DataFrame(when_data)
            else:
                data_frame = pd.DataFrame(when_data)

        model = self._internal_load(name)
        predictions = model.predict(data_frame, kwargs)

        pred_arr = []
        for i in range(len(predictions)):
            pred_arr.append({})
            pred_arr[-1] = {}
            for col in predictions.columns:
                pred_arr[-1][col] = {}
                pred_arr[-1][col]['predicted_value'] = predictions[col].iloc[i]

        return pred_arr

    def get_model_data(self, name):
        with open(os.path.join(self._dir(name), 'metadata.json'), 'r') as fp:
            return json.load(fp)

    def save_model_data(self, name, data):
        with open(os.path.join(self._dir(name), 'metadata.json'), 'w') as fp:
            json.dump(data, fp)

    def get_models(self):
        models = []
        for model_dir in os.listdir(self.storage_dir):
            if 'custom_model_' in model_dir:
                name = model_dir.replace('custom_model_','')
                try:
                    models.append(self.get_model_data(name))
                except:
                    print(f'Model {name} not found !')

        return models

    def delete_model(self, name):
        shutil.rmtree(self._dir(name))
        self.dbw.unregister_predictor(name)

    def rename_model(self, name, new_name):
        self.dbw.unregister_predictor(name)
        shutil.move(self._dir(name), self._dir(new_name))
        shutil.move(os.path.join(self._dir(new_name) + f'{name}.py'), os.path.join(self._dir(new_name) ,f'{new_name}.py'))
        self.dbw.register_predictors([self.get_model_data(new_name)])

    def export_model(self, name):
        shutil.make_archive(base_name=name, format='zip', root_dir=self._dir(name))
        return str(self._dir(name)) + '.zip'

    def load_model(self, fpath, name, trained_status):
        shutil.unpack_archive(fpath, self._dir(name), 'zip')
        shutil.move( os.path.join(self._dir(name), 'model.py') ,  os.path.join(self._dir(name), f'{name}.py') )
        model = self._internal_load(name)
        model.to_predict = model.to_predict if isinstance(model.to_predict,list) else [model.to_predict]
        self.save_model_data(name,{
            'name': name
            ,'data_analysis_v2': model.column_type_map
            ,'predict': model.to_predict
            ,'status': trained_status
            ,'is_custom': True
        })

        with open(os.path.join(self._dir(name), '__init__.py') , 'w') as fp:
            fp.write('')

        if trained_status == 'trained':
            self.dbw.register_predictors([self.get_model_data(name)])
