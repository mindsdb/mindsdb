import unittest
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import ModelStorage, HandlerStorage
from mindsdb.integrations.handlers.mlflow_handler.mlflow_handler import MLflowHandler
from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler


class MLflowHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.registered_model_name = 'nlp_kaggle'  # already saved to mlflow local instance
        cls.handler = MLflowHandler(ModelStorage(None, None), HandlerStorage(None, None))
        cls.config = Config()

        kwargs = {
            "host": "localhost",
            "port": "3306",
            "user": "root",
            "password": "root",
            "database": "test",
            "ssl": False
        }
        cls.sql_handler_name = 'test_handler'
        cls.data_table_name = 'train_escaped_csv'  # 'tweet_sentiment_train'
        cls.data_handler = MySQLHandler(cls.sql_handler_name, **kwargs)

    def test_1_create_predictor(self):
        args = {
            "model_name": self.registered_model_name,
            "mlflow_server_url": "http://0.0.0.0:5001/",
            "mlflow_server_path": "sqlite:////path/to/mlflow.db",
            'predict_url': "'http://localhost:5000/invocations'"
        }
        target_col = 'target'
        self.handler.create(target_col, args=args)

    def test_2_predict(self):
        self.handler.predict(pd.DataFrame.from_dict({'text': ['This is nice.']}))

    def test_3_describe_table(self):
        self.handler.describe(self.registered_model_name)
