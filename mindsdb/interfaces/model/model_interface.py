# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
import xmlrpc
import xmlrpc.client
import time
import pickle

from mindsdb.utilities.log import log


class ModelInterfaceRPC():
    def __init__(self):
        for _ in range(10):
            try:
                time.sleep(3)
                self.proxy = xmlrpc.client.ServerProxy("http://localhost:19329/", allow_none=True)
                assert self.proxy.ping()
                return
            except Exception:
                log.info('Wating for native RPC server to start')
        raise Exception('Unable to connect to RPC server')

    def create(self, *args, **kwargs):
        self.proxy.create(*args, **kwargs)

    def learn(self, *args, **kwargs):
        self.proxy.learn(*args, **kwargs)

    def predict(self, *args, **kwargs):
        bin = self.proxy.predict(*args, **kwargs)
        return pickle.loads(bin.data)

    def analyse_dataset(self, *args, **kwargs):
        bin = self.proxy.analyse_dataset(*args, **kwargs)
        return pickle.loads(bin.data)

    def get_model_data(self, *args, **kwargs):
        bin = self.proxy.get_model_data(*args, **kwargs)
        return pickle.loads(bin.data)

    def get_models(self, *args, **kwargs):
        bin = self.proxy.get_models(*args, **kwargs)
        return pickle.loads(bin.data)

    def delete_model(self, *args, **kwargs):
        self.proxy.delete_model(*args, **kwargs)

    def update_model(self, *args, **kwargs):
        return 'Model updating is no available in this version of mindsdb'


try:
    from mindsdb_worker.cluster.ray_interface import ModelInterfaceRay
    import ray
    ray.init(ignore_reinit_error=True)
    ModelInterface = ModelInterfaceRay
    ray_based = True
except Exception as e:
    ModelInterface = ModelInterfaceRPC
    ray_based = False
