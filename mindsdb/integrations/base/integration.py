from abc import ABC, abstractmethod
from mindsdb.utilities.config import Config

class Integration(ABC):
    def __init__(self, config, name):
        self.config = config
        self.name = name
        self.mindsdb_database = config['api']['mysql']['database']

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def _query(self, query, fetch=False):
        pass

    @abstractmethod
    def register_predictors(self, model_data_arr):
        pass

    @abstractmethod
    def unregister_predictor(self, name):
        pass
