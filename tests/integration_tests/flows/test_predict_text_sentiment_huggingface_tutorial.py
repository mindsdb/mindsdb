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


class QueryStorage:
    create_db = """
CREATE DATABASE example_db
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
    }; """
    check_db_created = """
SELECT *
FROM example_db.demo_data.user_comments LIMIT 3;
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
    prediction = """
SELECT * FROM sentiment_classifier
WHERE comment='It is really easy to do NLP with MindsDB';
    """
    bulk_prediction = """
SELECT input.comment, model.sentiment
FROM example_db.demo_data.user_comments AS input
JOIN sentiment_classifier AS model;
    """


@pytest.mark.usefixtures("mindsdb_app")
class TestPredictTextSentimentHuggingface(HTTPHelperMixin):

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
        assert len(resp['data']) == 3

    def test_create_model(self):
        sql = QueryStorage.create_model
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)

        assert len(resp['data']) == 1
        print(f"CREATE_MODEL_REPONSE - {resp}")
        status = resp['column_names'].index('STATUS')
        assert resp['data'][0][status] == 'generating'

    def test_wait_training_complete(self):
        status = self.await_model_by_query(QueryStorage.check_status, timeout=600)
        assert status == 'complete'
        # self.await_model("home_rentals_model", timeout=600)

    def test_prediction(self):
        sql = QueryStorage.prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 1

    def test_bulk_prediciton(self):
        sql = QueryStorage.bulk_prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) >= 1
