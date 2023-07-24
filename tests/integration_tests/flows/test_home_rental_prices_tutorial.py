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
FROM example_db.demo_data.home_rentals
LIMIT 10;
    """
    create_model = """
CREATE MODEL
  mindsdb.home_rentals_model
FROM example_db
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price;
    """
    check_status = """
SELECT *
FROM mindsdb.models
WHERE name='home_rentals_model';
    """
    prediction = """
SELECT rental_price,
       rental_price_explain
FROM mindsdb.home_rentals_model
WHERE sqft = 823
AND location='good'
AND neighborhood='downtown'
AND days_on_market=10;
    """
    bulk_prediction = """
SELECT t.rental_price as real_price,
m.rental_price as predicted_price,
t.number_of_rooms,  t.number_of_bathrooms, t.sqft, t.location, t.days_on_market
FROM example_db.demo_data.home_rentals as t
JOIN mindsdb.home_rentals_model as m limit 100;
    """
    finetune_model = """
FINETUNE mindsdb.home_rentals_model
FROM example_db (
    SELECT * FROM demo_data.home_rentals
) using join_learn_process=true;
    """


@pytest.mark.usefixtures("mindsdb_app")
class TestHomeRentalPrices(HTTPHelperMixin):

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
        assert len(resp['data']) == 10

    def test_create_model(self):
        sql = QueryStorage.create_model
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)

        assert len(resp['data']) == 1
        print(f"CREATE_MODEL_REPONSE - {resp}")
        status = resp['column_names'].index('STATUS')
        assert resp['data'][0][status] == 'generating'

    def test_wait_training_complete(self):
        self.await_model("home_rentals_model", timeout=600)

    def test_prediction(self):
        sql = QueryStorage.prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 1

    def test_bulk_prediciton(self):
        sql = QueryStorage.bulk_prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 100

    def test_finetune_model(self):
        sql = QueryStorage.finetune_model
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 1
        print(f"CREATE_MODEL_REPONSE - {resp}")
        status = resp['column_names'].index('STATUS')
        assert resp['data'][0][status] == 'complete'  # check it returns last 'complete' model version as current record
