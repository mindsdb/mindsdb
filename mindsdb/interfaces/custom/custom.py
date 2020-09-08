import os
import shutil
import importlib

from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.fs import get_or_create_dir_struct

class CustomModels():
    def __init__(self, config):
        self.config = config
        self.dbw = DatabaseWrapper(self.config)
        _, _, _, self.storage_dir = get_or_create_dir_struct()
        self.model_cache = []

    def _dir(self, name):
        os.path.join(self.storage_dir, 'custom_model_' + name)

    def internal_load(self, name):
        model = importlib.import_moduel(self._dir(name) + '/model.py')

        if name in self.model_cache:
            return self.model_cache[name]

        if hasattr(model, 'setup'):
            model.setup()

        self.model_cache[name] = model

        return model

    def learn(self, name, from_data, to_predict, kwargs={}):
        model = internal_load(name)
        model.fit(name, from_data, to_predict, kwargs)

    def predict(self, name, when_data=None, kwargs={}):
        model = internal_load(name)
        predictions = model.predict(when_data, kwargs)
        return predictions

    def get_model_data(self, name):
        pass

    def get_models(self, status='any'):
        models = []
        for dir in os.listdir(self.storage_dir):
            if 'custom_model_' in dir:
                models.append({
                    'name': dir.replace('custom_model_', '')
                })

        return models

    def delete_model(self, name):
        shutil.rmtree(self._dir(name))
        self.dbw.unregister_predictor(name)

    def rename_model(self, name, new_name):
        shutil.move(self._dir(name), self._dir(new_name))

    def load_model(self, fpath, name):
        shutil.unpack_archive(fpath,self._dir(name), 'zip')
