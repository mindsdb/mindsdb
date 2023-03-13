import os
from pathlib import Path
import json
import pytest

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from .conftest import CONFIG_PATH
from .http_test_helpers import HTTPHelperMixin


# used by mindsdb_app fixture in conftest
OVERRIDE_CONFIG = {
    'integrations': {},
}
# used by (required for) mindsdb_app fixture in conftest
API_LIST = ["http", ]
OPEN_AI_API_KEY = os.environ.get("OPEN_AI_API_KEY")


class QueryStorage:
    create_db = """
CREATE DATABASE mysql_demo_db
WITH ENGINE = "mysql",
PARAMETERS = {
    "user": "user",
    "password": "MindsDBUser123!",
    "host": "db-demo-data.cwoyhfn6bzs0.us-east-1.rds.amazonaws.com",
    "port": "3306",
    "database": "public"
    };
"""

    check_db_created = """
SELECT *
FROM mysql_demo_db.amazon_reviews LIMIT 3;
"""

    create_engine = """
CREATE ML_ENGINE openai
 FROM openai USING api_key='%s';
    """

    create_model = """
CREATE MODEL sentiment_classifier_gpt3
PREDICT sentiment
USING
engine = 'openai',
prompt_template = 'describe the sentiment of the reviews
strictly as "positive", "neutral", or "negative".
"I love the product":positive
"It is a scam":negative
"{{review}}.":',
api_key = '%s';
"""

    check_status = """
SELECT * FROM models
WHERE name = 'sentiment_classifier_gpt3';
"""

    prediction = """
SELECT review, sentiment
FROM sentiment_classifier_gpt3
WHERE review = 'It is ok.';
"""

    bulk_prediction = """
SELECT input.review, output.sentiment
FROM mysql_demo_db.amazon_reviews AS input
JOIN sentiment_classifier_gpt3 AS output
LIMIT 5;
"""


@pytest.mark.usefixtures("mindsdb_app")
class TestPredictTextSentimentOpenAI(HTTPHelperMixin):

    @classmethod
    def setup_class(cls):
        cls.config = json.loads(
            Path(CONFIG_PATH).read_text()
        )

        cls.initial_integrations_names = list(cls.config['integrations'].keys())

    def test_create_db(self):
        sql = QueryStorage.create_db
        self.sql_via_http(sql, RESPONSE_TYPE.OK)

    def test_db_created(self):
        sql = QueryStorage.check_db_created
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) >= 3

    def test_create_engine(self):
        sql = QueryStorage.create_engine % OPEN_AI_API_KEY
        self.sql_via_http(sql, RESPONSE_TYPE.OK)

    def test_create_model(self):
        sql = QueryStorage.create_model % OPEN_AI_API_KEY
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)

        assert len(resp['data']) == 1
        status = resp['column_names'].index('STATUS')
        assert resp['data'][0][status] == 'generating'

    def test_wait_training_complete(self):
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
