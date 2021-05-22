import time
import pickle
import os

from mindsdb.utilities.log import log
import pyarrow.flight as fl


class ModelInterfaceWrapper(object):
    def __init__(self, model_interface, company_id=None):
        self.company_id = company_id
        self.model_interface = model_interface

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            if kwargs.get('company_id') is None:
                kwargs['company_id'] = self.company_id
            return getattr(self.model_interface, name)(*args, **kwargs)
        return wrapper


class ModelInterfaceRPC():
    def __init__(self):
        for _ in range(10):
            try:
                time.sleep(3)
                self.client = fl.connect("grpc://localhost:19329")
                res = self._action('ping')
                assert self._loads_first(res)
                return
            except Exception:
                log.info('Wating for native RPC server to start')
        raise Exception('Unable to connect to RPC server')

    def _action(self, act_name, *args, **kwargs):
        action = fl.Action(act_name, pickle.dumps({'args': args, 'kwargs': kwargs}))
        return list(self.client.do_action(action))

    def _loads_first(self, res):
        return pickle.loads(res[0].body.to_pybytes())

    def create(self, *args, **kwargs):
        self._action('create', *args, **kwargs)

    def learn(self, *args, **kwargs):
        self._action('learn', *args, **kwargs)

    def predict(self, *args, **kwargs):
        res = self._action('predict', *args, **kwargs)
        return self._loads_first(res)

    def analyse_dataset(self, *args, **kwargs):
        res = self._action('analyse_dataset', *args, **kwargs)
        return self._loads_first(res)

    def get_model_data(self, *args, **kwargs):
        res = self._action('get_model_data', *args, **kwargs)
        return self._loads_first(res)

    def get_models(self, *args, **kwargs):
        res = self._action('get_models', *args, **kwargs)
        return self._loads_first(res)

    def delete_model(self, *args, **kwargs):
        self._action('delete_model', *args, **kwargs)

    def update_model(self, *args, **kwargs):
        return 'Model updating is no available in this version of mindsdb'

try:
    from mindsdb_worker.cluster.ray_interface import ModelInterfaceRay
    import ray
    try:
        ray.init(ignore_reinit_error=True, address='auto')
    except Exception:
        ray.init(ignore_reinit_error=True)
    ModelInterface = ModelInterfaceRay
    ray_based = True
except Exception as e:
    log.error(f'Failed to import ray: {e}')
    ModelInterface = ModelInterfaceRPC
    ray_based = False
