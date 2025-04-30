from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.utils.http_test_helpers import HTTPHelperMixin


class QueryStorage:
    create_db = """
CREATE DATABASE example_sentiment_huggingface_db
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "samples.mindsdb.com",
    "port": "5432",
    "database": "demo"
    };
"""
    check_db_created = """
SELECT *
FROM example_sentiment_huggingface_db.demo_data.user_comments LIMIT 3;
"""
    delete_db = """
DROP DATABASE example_sentiment_huggingface_db;
"""
    create_model = """
CREATE MODEL sentiment_classifier
PREDICT sentiment
USING engine='huggingface',
  task = 'text-classification',
  model_name= 'cardiffnlp/twitter-roberta-base-sentiment',
  input_column = 'comment',
  labels=['negative','neutral','positive'];
"""
    check_status = """
SELECT *
FROM models
WHERE name = 'sentiment_classifier';
"""
    delete_model = """
DROP MODEL
  mindsdb.sentiment_classifier;
"""
    prediction = """
SELECT * FROM sentiment_classifier
WHERE comment='It is really easy to do NLP with MindsDB';
"""
    bulk_prediction = """
SELECT input.comment, model.sentiment
FROM example_sentiment_huggingface_db.demo_data.user_comments AS input
JOIN sentiment_classifier AS model;
"""


class TestPredictTextSentimentHuggingface(HTTPHelperMixin):

    def setup_class(self):
        self.sql_via_http(self, QueryStorage.delete_db)
        self.sql_via_http(self, QueryStorage.delete_model)

    def test_create_db(self):
        sql = QueryStorage.create_db
        self.sql_via_http(sql, RESPONSE_TYPE.OK)

    def test_db_created(self):
        sql = QueryStorage.check_db_created
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 3

    def test_create_model(self, train_finetune_lock):
        with train_finetune_lock.acquire(timeout=600):
            sql = QueryStorage.create_model
            resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
            assert len(resp['data']) == 1
            status = resp['column_names'].index('STATUS')
            assert resp['data'][0][status] == 'generating'
            status = self.await_model_by_query(QueryStorage.check_status, timeout=600)
            assert status == 'complete'

    def test_prediction(self):
        sql = QueryStorage.prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 1

    def test_bulk_prediciton(self):
        sql = QueryStorage.bulk_prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) >= 1
