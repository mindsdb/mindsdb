import unittest

from mindsdb.utilities.config import Config
from mindsdb.integrations.mlflow_handler.mlflow.mlflow_handler import MLflowHandler
from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import MySQLHandler


class MLflowHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.registered_model_name = 'nlp_kaggle3'  # already saved to mlflow local instance
        cls.handler = MLflowHandler('test_mlflow')
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

    def test_1_connect(self):
        self.assertTrue(
            self.handler.connect(
                mlflow_server_url='http://127.0.0.1:5001',  # for this test, serve at 5001 and served model at 5000
                model_registry_path='sqlite:///../../../../../temp/experiments/BYOM/mlflow.db',
                config={
                    'path': self.config['paths']['root'],
                    'name': 'test_name'
                }
            ) == {'status': '200'}
        )

    def test_2_drop_predictor(self):
        try:
            self.handler.run_native_query(f"DROP PREDICTOR {self.registered_model_name}")
        except:
            query = f"CREATE PREDICTOR {self.registered_model_name} PREDICT target USING url.predict='http://localhost:5000/invocations'"
            self.handler.run_native_query(query)
            self.test_2_drop_predictor()

    def test_3_create_predictor(self):
        query = f"CREATE PREDICTOR {self.registered_model_name} PREDICT target USING url.predict='http://localhost:5000/invocations'"
        self.handler.run_native_query(query)

    def test_4_get_tables(self):
        self.handler.get_tables()

    def test_5_describe_table(self):
        self.handler.describe_table(f'{self.registered_model_name}')

    def test_6_select_from(self):
        self.assertTrue(self.data_handler.check_connection())

        query = f"SELECT target from {self.registered_model_name} WHERE text='This is nice.'"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.select_query(parsed)

    def test_7_join(self):
        self.assertTrue(self.data_handler.check_connection())

        into_table = 'test_join_into_mlflow'
        query = f"SELECT tb.target as predicted, ta.target as real, tb.text from {self.sql_handler_name}.{self.data_table_name} AS ta JOIN {self.registered_model_name} AS tb LIMIT 10"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        predicted = self.handler.join(parsed, self.data_handler, into=into_table)

        q = f"SELECT * FROM {into_table}"
        qp = self.handler.parser(q, dialect='mysql')
        assert len(self.data_handler.select_query(qp.targets, qp.from_table, qp.where)) > 0

        try:
            self.data_handler.run_native_query(f"DROP TABLE test.{into_table}")
        except:
            pass
