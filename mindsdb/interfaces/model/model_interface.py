# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
import xmlrpc.client
import time

from mindsdb.utilities.config import Config
from mindsdb.utilities.log import log

try:
    import ray
    ray_based = True
except:
    ray_based = False

if ray_based:
    class ModelInterface():
        def __init__(self):
            from mindsdb.interfaces.model.model_controller import ModelController

            self.config = Config()
            self.controller = ModelController.remote()

        def create(self, name):
            fut = self.controller.create.remote(name)
            return ray.get(fut)

        def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
            fut = self.controller.learn.remote(name, from_data, to_predict, datasource_id, kwargs)
            return ray.get(fut)

        def predict(self, name, when_data=None, kwargs={}):
            fut = self.controller.predict.remote(name, when_data, kwargs)
            ray.get(fut)

        def analyse_dataset(self, ds):
            fut = self.controller.analyse_dataset.remote(ds)
            ray.get(fut)

        def get_model_data(self, name, db_fix=True):
            fut = self.controller.analyse_dataset.remote(name, db_fix)
            ray.get(fut)

        def get_models(self):
            fut = self.controller.get_models.remote()
            ray.get(fut)

        def delete_model(self, name):
            fut = self.controller.delete_model.remote(name)
            ray.get(fut)
            
else:
    class ModelInterface():
        def __init__(self):
            self.config = Config()
            for _ in range(30):
                try:
                    time.sleep(3)
                    self.proxy = xmlrpc.client.ServerProxy("http://localhost:17329/")
                    assert self.proxy.ping()
                    return
                except:
                    log.info('Wating for native RPC server to start')
            raise Exception('Unable to connect to RPC server')


        def create(self, name):
            return self.proxy.create(name)

        def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
            return self.proxy.learn(name, from_data, to_predict, datasource_id, kwargs)

        def predict(self, name, when_data=None, kwargs={}):
            return self.proxy.predict(name, when_data, kwargs)

        def analyse_dataset(self, ds):
            return self.proxy.analyse_dataset(ds)

        def get_model_data(self, name, db_fix=True):
            return self.proxy.analyse_dataset(name, db_fix)

        def get_models(self):
            return self.proxy.get_models()

        def delete_model(self, name):
            return self.proxy.delete_model(name)
