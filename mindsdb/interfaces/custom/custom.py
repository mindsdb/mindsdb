import os
import shutil

from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.fs import get_or_create_dir_struct

class CustomModels():
    def __init__(self, config):
        self.config = config
        self.dbw = DatabaseWrapper(self.config)
        _, _, _, self.storage_dir = get_or_create_dir_struct()

    def learn(self, name, from_data, to_predict, kwargs={}):
        pass

    def predict(self, name, when_data=None, kwargs={}):
        pass

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
        shutil.rmtree(os.path.join(self.storage_dir, 'custom_model_' + name))
        self.dbw.unregister_predictor(name)

    def rename_model(self, name, new_name):
        shutil.move(os.path.join(self.storage_dir, 'custom_model_' + name) , os.path.join(self.storage_dir, 'custom_model_' + new_name))

    def load_model(self, fpath, name):
        shutil.unpack_archive(fpath, os.path.join(self.storage_dir, 'custom_model_' + name), 'zip')
