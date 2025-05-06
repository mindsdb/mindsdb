from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.utils.http_test_helpers import HTTPHelperMixin


class QueryStorage:
    create_db = """
CREATE DATABASE example_quarterly_house_db
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "samples.mindsdb.com",
    "port": "5432",
    "database": "demo"
    };
"""
    delete_db = """
DROP DATABASE example_quarterly_house_db;
"""
    check_db_created = """
SELECT *
FROM example_quarterly_house_db.demo_data.house_sales
LIMIT 10;
"""
    create_model = """
CREATE MODEL
  mindsdb.house_sales_model
FROM example_quarterly_house_db
  (SELECT * FROM demo_data.house_sales)
PREDICT ma
ORDER BY saledate
GROUP BY bedrooms, type
WINDOW 8
HORIZON 4;
"""
    delete_model = """
DROP MODEL
  mindsdb.house_sales_model;
"""
    check_status = """
SELECT *
FROM mindsdb.models
WHERE name='house_sales_model';
"""
    prediction = """
SELECT m.saledate as date, m.ma as forecast
  FROM mindsdb.house_sales_model as m
  JOIN example_quarterly_house_db.demo_data.house_sales as t
  WHERE t.saledate > LATEST AND t.type = 'house'
  AND t.bedrooms=2
  LIMIT 4;
"""


class TestForecastQuaterlyHouseSales(HTTPHelperMixin):

    def setup_class(self):
        self.sql_via_http(self, QueryStorage.delete_db)
        self.sql_via_http(self, QueryStorage.delete_model)

    def test_create_db(self):
        self.sql_via_http(QueryStorage.create_db, RESPONSE_TYPE.OK)

    def test_db_created(self):
        sql = QueryStorage.check_db_created
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 10

    def test_create_model(self, train_finetune_lock):
        with train_finetune_lock.acquire(timeout=600):
            resp = self.sql_via_http(QueryStorage.create_model, RESPONSE_TYPE.TABLE)
            assert len(resp['data']) == 1
            status = resp['column_names'].index('STATUS')
            assert resp['data'][0][status] == 'generating'
            self.await_model("house_sales_model", timeout=600)

    def test_prediction(self):
        sql = QueryStorage.prediction
        resp = self.sql_via_http(sql, RESPONSE_TYPE.TABLE)
        assert len(resp['data']) == 4
