import unittest
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.mlflow_handler.mlflow_handler.mlflow_handler import MLflowHandler
from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.interfaces.storage.fs import ModelStorage, HandlerStorage


class MLflowHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.registered_model_name = 'nlp_kaggle3'  # already saved to mlflow local instance
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

    def __random(self):
        self.handler.connect(
            mlflow_server_url='http://127.0.0.1:5001',  # for this test, serve at 5001 and served model at 5000
            model_registry_path='sqlite:///../../../../../temp/experiments/BYOM/mlflow.db',
            config={
                'path': self.config['paths']['root'],
                'name': 'test_name'
            }
        )

    def test_3_create_predictor(self):
        self.handler.create(self.registered_model_name,
                            'target',
                            args={'predict_url': "'http://localhost:5000/invocations'"})

    def test_5_describe_table(self):
        df = self.handler.describe_table(self.registered_model_name)

    def test_6_predict(self):
        df = self.handler.predict(pd.DataFrame.from_dict({'text': ['This is nice.']}))
