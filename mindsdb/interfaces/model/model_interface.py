# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
import xmlrpc
import xmlrpc.client
import time
import pickle

from mindsdb.utilities.log import log

try:
    import ray
    ray.init(ignore_reinit_error=True)
    ray_based = True
except:
    ray_based = False

class ModelInterfaceRPC():
    def __init__(self):
        for _ in range(10):
            try:
                time.sleep(3)
                self.proxy = xmlrpc.client.ServerProxy("http://localhost:19329/", allow_none=True)
                assert self.proxy.ping()
                return
            except:
                log.info('Wating for native RPC server to start')
        raise Exception('Unable to connect to RPC server')

    def create(self, name):
        self.proxy.create(name)

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        self.proxy.learn(name, from_data, to_predict, datasource_id, kwargs)

    def predict(self, name, pred_format, when_data=None, kwargs={}):
        bin = self.proxy.predict(name, pred_format, when_data, kwargs)
        return pickle.loads(bin.data)

    def analyse_dataset(self, ds):
        bin = self.proxy.analyse_dataset(ds)
        return pickle.loads(bin.data)

    def get_model_data(self, name, db_fix=True):
        bin = self.proxy.get_model_data(name, db_fix)
        return pickle.loads(bin.data)

    def get_models(self):
        bin = self.proxy.get_models()
        return pickle.loads(bin.data)

    def delete_model(self, name):
        self.proxy.delete_model(name)

class ModelInterfaceRay():
    def __init__(self):
        from mindsdb.interfaces.model.model_controller import ModelController
        try:
            self.controller = ray.get_actor("ModeControllerActor")
        except:
            self.controller = ModelController.options(name='ModeControllerActor').remote()

    def create(self, name):
        self.controller.create.remote(name)

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        join_learn_process = kwargs.get('join_learn_process', False)
        fut = self.controller.learn.remote(name, from_data, to_predict, datasource_id, kwargs)
        if join_learn_process:
            ray.get(fut)

    def predict(self, name, pred_format, when_data=None, kwargs={}):
        fut = self.controller.predict.remote(name, pred_format, when_data, kwargs)
        return ray.get(fut)

    def analyse_dataset(self, ds):
        fut = self.controller.analyse_dataset.remote(ds)
        return ray.get(fut)

    def get_model_data(self, name, db_fix=True):
        fut = self.controller.get_model_data.remote(name, db_fix)
        return ray.get(fut)

    def get_models(self):
        fut = self.controller.get_models.remote()
        return ray.get(fut)

    def delete_model(self, name):
        fut = self.controller.delete_model.remote(name)
        ray.get(fut)

if ray_based:
    ModelInterface = ModelInterfaceRay
else:
    ModelInterface = ModelInterfaceRPC
