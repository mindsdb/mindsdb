import os
import uuid

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.integration.utils.http_test_helpers import HTTPHelperMixin


OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

random_str = uuid.uuid4().hex[:6]
example_sentiment_openai_db = f"example_sentiment_openai_db_{random_str}"
openai_engine = f"openai2_{random_str}"
model_name = f"sentiment_classifier_gpt3_{random_str}"


class QueryStorage:
    create_db = f"""
CREATE DATABASE {example_sentiment_openai_db}
WITH ENGINE = "postgres",
PARAMETERS = {{
    "user": "demo_user",
    "password": "demo_password",
    "host": "samples.mindsdb.com",
    "port": "5432",
    "database": "demo",
    "schema": "demo_data"
    }};
"""
    check_db_created = f"""
SELECT *
FROM {example_sentiment_openai_db}.amazon_reviews LIMIT 3;
"""
    delete_db = f"""
DROP DATABASE {example_sentiment_openai_db};
"""
    create_engine = f"""
CREATE ML_ENGINE {openai_engine}
FROM openai USING openai_api_key='%s';
"""
    delete_engine = f"""
DROP ML_ENGINE {openai_engine};
"""
    create_model = f"""
CREATE MODEL {model_name}
PREDICT sentiment
USING
engine = '{openai_engine}',
prompt_template = 'describe the sentiment of the reviews
strictly as "positive", "neutral", or "negative".
"I love the product":positive
"It is a scam":negative
"{{review}}.":',
openai_api_key = '%s';
"""
    check_status = f"""
SELECT * FROM models
WHERE name = '{model_name}';
"""
    delete_model = f"""
DROP MODEL
  mindsdb.{model_name};
"""
    prediction = f"""
SELECT review, sentiment
FROM {model_name}
WHERE review = 'It is ok.';
"""
    bulk_prediction = f"""
SELECT input.review, output.sentiment
FROM {example_sentiment_openai_db}.amazon_reviews AS input
JOIN {model_name} AS output
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
        assert len(resp["data"]) >= 3

    def test_create_engine(self):
        sql = QueryStorage.create_engine % OPENAI_API_KEY
        self.sql_via_http(sql, RESPONSE_TYPE.OK)

    def test_create_model(self, train_finetune_lock):
        with train_finetune_lock.acquire(timeout=600):
            sql = QueryStorage.create_model % OPENAI_API_KEY
            resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
            assert len(resp["data"]) == 1
            status = resp["column_names"].index("STATUS")
            assert resp["data"][0][status] == "generating"
            status = self.await_model_by_query(QueryStorage.check_status, timeout=600)
            assert status == "complete"

    def test_prediction(self):
        sql = QueryStorage.prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp["data"]) == 1

    def test_bulk_prediciton(self):
        sql = QueryStorage.bulk_prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp["data"]) == 5
