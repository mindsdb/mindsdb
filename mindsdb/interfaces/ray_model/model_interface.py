import ray

from mindsdb.utilities.config import Config
from mindsdb.utilities.log import log
from mindsdb.interfaces.model.model_controller import ModelController

class ModelInterface():
    def __init__(self):
        self.config = Config()
        self.controller = ModelController.remote()

    def create(self, name):
        fut = self.controller.create.remote(name)
        return ray.get(fut)

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']
    
        fut = self.controller.learn.remote(name, from_data, to_predict, datasource_id, kwargs)
        if join_learn_process:
            return ray.get(fut)

    def predict(self, name, when_data=None, kwargs={}):
        fut = self.controller.predict.remote(name, when_data, kwargs)
        return ray.get(fut)

    def analyse_dataset(self, ds):
        fut = self.controller.analyse_dataset.remote(ds)
        return ray.get(fut)

    def get_model_data(self, name, db_fix=True):
        fut = self.controller.analyse_dataset.remote(name, db_fix)
        return ray.get(fut)

    def get_models(self):
        fut = self.controller.get_models.remote()
        return ray.get(fut)

    def delete_model(self, name):
        fut = self.controller.delete_model.remote(name)
        return ray.get(fut)
