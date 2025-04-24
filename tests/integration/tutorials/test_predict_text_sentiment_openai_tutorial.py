import os

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.utils.http_test_helpers import HTTPHelperMixin


OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


class QueryStorage:
    create_db = """
CREATE DATABASE example_sentiment_openai_db
WITH ENGINE = "mysql",
PARAMETERS = {
    "user": "user",
    "password": "MindsDBUser123!",
    "host": "samples.mindsdb.com",
    "port": "3306",
    "database": "public"
    };
"""
    check_db_created = """
SELECT *
FROM example_sentiment_openai_db.amazon_reviews LIMIT 3;
"""
    delete_db = """
DROP DATABASE example_sentiment_openai_db;
"""
    create_engine = """
CREATE ML_ENGINE openai2
FROM openai USING openai_api_key='%s';
"""
    delete_engine = """
DROP ML_ENGINE openai2;
"""
    create_model = """
CREATE MODEL sentiment_classifier_gpt3
PREDICT sentiment
USING
engine = 'openai2',
prompt_template = 'describe the sentiment of the reviews
strictly as "positive", "neutral", or "negative".
"I love the product":positive
"It is a scam":negative
"{{review}}.":',
openai_api_key = '%s';
"""
    check_status = """
SELECT * FROM models
WHERE name = 'sentiment_classifier_gpt3';
"""
    delete_model = """
DROP MODEL
  mindsdb.sentiment_classifier_gpt3;
"""
    prediction = """
SELECT review, sentiment
FROM sentiment_classifier_gpt3
WHERE review = 'It is ok.';
"""
    bulk_prediction = """
SELECT input.review, output.sentiment
FROM example_sentiment_openai_db.amazon_reviews AS input
JOIN sentiment_classifier_gpt3 AS output
LIMIT 5;
"""


class TestPredictTextSentimentOpenAI(HTTPHelperMixin):

    def setup_class(self):
        self.sql_via_http(self, QueryStorage.delete_db)
        self.sql_via_http(self, QueryStorage.delete_model)
        self.sql_via_http(self, QueryStorage.delete_engine)

    def test_create_db(self):
        sql = QueryStorage.create_db
        self.sql_via_http(sql, RESPONSE_TYPE.OK)

    def test_db_created(self):
        sql = QueryStorage.check_db_created
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) >= 3

    def test_create_engine(self):
        sql = QueryStorage.create_engine % OPENAI_API_KEY
        self.sql_via_http(sql, RESPONSE_TYPE.OK)

    def test_create_model(self, train_finetune_lock):
        with train_finetune_lock.acquire(timeout=600):
            sql = QueryStorage.create_model % OPENAI_API_KEY
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
        assert len(resp['data']) == 5
