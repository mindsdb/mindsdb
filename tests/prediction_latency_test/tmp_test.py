import os
import time
import importlib.util
import requests
from config import CONFIG


class Dataset:
    def __init__(self, name, **kwargs):
        self.name = name
        self.target = kwargs.get("target")
        self.handler_file = kwargs.get("handler_file", None)
        if self.handler_file is not None:
            self.handler = self._get_handler()
        else:
            self.handler = None

    def _get_handler(self):
        spec = importlib.util.spec_from_file_location("common", os.path.abspath(self.handler_file))
        handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler)
        return handler.handler

datasets = [Dataset(key, **CONFIG['datasets'][key]) for key  in CONFIG['datasets'].keys()]

class Predictor():
    def __init__(self, name):
        self.name = name
        self.base_url = "http://127.0.0.1:47334/api"
    def get_info(self):
        return requests.get(f'{self.base_url}/predictors/{self.name}').json()
    def is_ready(self):
        return self.get_info()["status"] != 'complete'

    def learn(self, to_predict):
        datasource_name = f"{self.name}_train"
        res = requests.put(f'{self.base_url}/predictors/{self.name}', json={
            'data_source_name': datasource_name,
            'to_predict': to_predict
        })
        res.raise_for_status()


def create_predictors():
    predictors = []
    for dataset in datasets:
        predictor = Predictor(dataset.name)
        print(f"initial {dataset.name} predictor state: {predictor.get_info()}")
        print(f"creating {dataset.name} predictor")
        predictor.learn(dataset.target)

    while True:
        for predictor in predictors[:]:
            if predictor.is_ready():
                print(f"predictor {predictor.name} is ready")
                print(predictor.get_info())
                predictors.remove(predictor)
            time.sleep(5)


if __name__ == '__main__':
    create_predictors()
