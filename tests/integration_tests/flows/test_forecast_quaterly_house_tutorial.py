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
    };
"""

    check_db_created = """
SELECT *
FROM example_db.demo_data.house_sales
LIMIT 10;
"""

    create_model = """
CREATE MODEL
  mindsdb.house_sales_model
FROM example_db
  (SELECT * FROM demo_data.house_sales)
PREDICT ma
ORDER BY saledate
GROUP BY bedrooms, type
WINDOW 8
HORIZON 4;
"""
    check_status = """
SELECT *
FROM mindsdb.models
WHERE name='house_sales_model';
"""
    prediction = """
SELECT m.saledate as date, m.ma as forecast
  FROM mindsdb.house_sales_model as m
  JOIN example_db.demo_data.house_sales as t
  WHERE t.saledate > LATEST AND t.type = 'house'
  AND t.bedrooms=2
  LIMIT 4;
"""


@pytest.mark.usefixtures("mindsdb_app")
class TestForecastQuaterlyHouseSales(HTTPHelperMixin):

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
        self.await_model("house_sales_model", timeout=600)

    def test_prediction(self):
        sql = QueryStorage.prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 4
