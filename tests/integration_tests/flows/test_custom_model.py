import unittest
import shutil
import requests
from pathlib import Path
import textwrap
import json

import mysql.connector

from common import (
    MINDSDB_DATABASE,
    CONFIG_PATH,
    TEMP_DIR,
    HTTP_API_ROOT,
    run_environment
)

# +++ define test data
TEST_DATASET = 'home_rentals'

DB_TYPES_MAP = {
    int: 'int',
    float: 'float',
    str: 'varchar(255)'
}

TO_PREDICT = {
    'rental_price': float,
    'location': str
}
CONDITION = {
    'sqft': 1000,
    'neighborhood': 'downtown'
}
# ---

root = 'http://127.0.0.1:47334/api'

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

INTEGRATION_NAME = 'default_mariadb'

config = {}


def query(q, as_dict=False, fetch=False):
    con = mysql.connector.connect(
        host=config['integrations'][INTEGRATION_NAME]['host'],
        port=config['integrations'][INTEGRATION_NAME]['port'],
        user=config['integrations'][INTEGRATION_NAME]['user'],
        passwd=config['integrations'][INTEGRATION_NAME]['password'],
        db=MINDSDB_DATABASE,
        connect_timeout=1000
    )

    cur = con.cursor(dictionary=as_dict)
    cur.execute(q)
    res = True
    if fetch:
        res = cur.fetchall()
    con.commit()
    con.close()
    return res


def fetch(q, as_dict=True):
    return query(q, as_dict, fetch=True)


class CustomModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['mysql', 'http'],
            override_config={
                'integrations': {
                    INTEGRATION_NAME: {
                        'publish': True
                    }
                }
            }
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

    def test_0_put_external_ds(self):
        params = {
            'name': EXTERNAL_DS_NAME,
            'query': f'select * from test_data.{TEST_DATA_TABLE} limit 50',
            'integration_id': INTEGRATION_NAME
        }

        url = f'{HTTP_API_ROOT}/datasources/{EXTERNAL_DS_NAME}'
        res = requests.put(url, json=params)
        self.assertTrue(res.status_code == 200)
        ds_data = res.json()

        self.assertTrue(ds_data['source_type'] == INTEGRATION_NAME)
        self.assertTrue(ds_data['row_count'] == 50)

        url = f'{HTTP_API_ROOT}/datasources'
        res = requests.get(url)
        self.assertTrue(res.status_code == 200)
        ds_data = res.json()
        self.assertTrue(len(ds_data) == 1)

    def test_1_simple_model_upload(self):
        model_dir = TEMP_DIR.joinpath('test_custom_model/')
        if model_dir.is_dir():
            shutil.rmtree(str(model_dir))
        model_dir.mkdir(parents=True, exist_ok=True)

        with open(str(model_dir.joinpath('model.py')), 'w') as fp:
            fp.write(textwrap.dedent("""
                from sklearn.linear_model import LinearRegression
                import numpy as np
                import pandas as pd
                from mindsdb import ModelInterface

                class Model(ModelInterface):
                    def setup(self):
                        print('Setting up model !')
                        self.model = LinearRegression()

                    def get_x(self, data):
                        initial_price = np.array([int(x) for x in data['initial_price']])
                        initial_price = initial_price.reshape(-1, 1)
                        return initial_price

                    def get_y(self, data, to_predict_str):
                        to_predict = np.array(data[to_predict_str])
                        return to_predict

                    def predict(self, from_data, kwargs):
                        initial_price = self.get_x(from_data)
                        rental_price = self.model.predict(initial_price)
                        df = pd.DataFrame({self.to_predict[0]: rental_price})
                        return df

                    def fit(self, from_data, to_predict, data_analysis, kwargs):
                        self.model = LinearRegression()
                        Y = self.get_y(from_data, to_predict[0])
                        X = self.get_x(from_data)
                        self.model.fit(X, Y)
        """))

        shutil.make_archive(base_name='my_model', format='zip', root_dir=str(model_dir))

        # Upload the model (new endpoint)
        res = requests.put(f'{root}/predictors/custom/{TEST_PREDICTOR_NAME}', files=dict(file=open('my_model.zip', 'rb')), json={
            'trained_status': 'untrained'
        })
        print(res.status_code)
        print(res.text)
        assert res.status_code == 200

        # Train the model (new endpoint, just redirects to the /predictors/ endpoint basically)
        params = {
            'data_source_name': EXTERNAL_DS_NAME,
            'to_predict': 'rental_price',
            'kwargs': {}
        }
        res = requests.post(f'{root}/predictors/{TEST_PREDICTOR_NAME}/learn', json=params)
        print(res.status_code)
        print(res.text)
        assert res.status_code == 200

        # Run a single value prediction
        params = {
            'when': {'initial_price': 5000}
        }
        url = f'{root}/predictors/{TEST_PREDICTOR_NAME}/predict'
        res = requests.post(url, json=params)
        assert res.status_code == 200
        assert isinstance(res.json()[0]['rental_price']['predicted_value'], float)
        assert 4500 < res.json()[0]['rental_price']['predicted_value'] < 5500

        params = {
            'data_source_name': EXTERNAL_DS_NAME
        }
        url = f'{root}/predictors/{TEST_PREDICTOR_NAME}/predict_datasource'
        res = requests.post(url, json=params)

        assert res.status_code == 200
        assert(len(res.json()) == 50)
        for pred in res.json():
            assert isinstance(pred['rental_price']['predicted_value'], float)

    def test_2_db_predict_from_external_datasource(self):
        res = fetch(
            f"""
                SELECT rental_price
                FROM {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
                WHERE external_datasource='{EXTERNAL_DS_NAME}'
            """,
            as_dict=True
        )

        self.assertTrue(len(res) > 0)
        self.assertTrue(res[0]['rental_price'] is not None and res[0]['rental_price'] != 'None')

    def test_3_retrain_model(self):
        query(f"""
            INSERT INTO {MINDSDB_DATABASE}.predictors (name, predict, select_data_query)
            VALUES ('{TEST_PREDICTOR_NAME}', 'sqft', 'SELECT * FROM test_data.{TEST_DATA_TABLE} where sqft is not null')
        """)

    def test_4_predict_with_retrained_from_sql(self):
        res = fetch(f"""SELECT sqft FROM {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME} WHERE initial_price=6000""", as_dict=True)

        self.assertTrue(len(res) > 0)
        print(res)
        self.assertTrue(res[0]['sqft'] is not None and res[0]['sqft'] != 'None')

    def test_5_predict_with_retrained_from_select(self):
        res = fetch(
            f"""
                SELECT sqft
                FROM {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
                WHERE select_data_query='SELECT * FROM test_data.{TEST_DATA_TABLE}'
            """,
            as_dict=True
        )

        self.assertTrue(len(res) > 0)
        print(res)
        self.assertTrue(res[0]['sqft'] is not None and res[0]['sqft'] != 'None')

    def test_6_predict_with_retrained_from_http_api(self):
        params = {
            'when': {'initial_price': 5000, 'rental_price': 4000}
        }
        url = f'{root}/predictors/{TEST_PREDICTOR_NAME}/predict'
        res = requests.post(url, json=params)
        assert res.status_code == 200
        assert isinstance(res.json()[0]['sqft']['predicted_value'], float)

        params = {
            'data_source_name': EXTERNAL_DS_NAME
        }
        url = f'{root}/predictors/{TEST_PREDICTOR_NAME}/predict_datasource'
        res = requests.post(url, json=params)

        assert res.status_code == 200
        assert(len(res.json()) == 50)
        for pred in res.json():
            assert isinstance(pred['sqft']['predicted_value'], float)

    def test_7_utils_from_http_api(self):
        res = requests.get(f'{root}/predictors')
        assert res.status_code == 200
        assert TEST_PREDICTOR_NAME in [x['name'] for x in res.json()]
        for ele in res.json():
            print(ele)
            if ele['name'] == TEST_PREDICTOR_NAME:
                assert ele['is_custom'] is True

        res = requests.get(f'{root}/predictors/{TEST_PREDICTOR_NAME}')
        assert res.status_code == 200
        assert res.json()['name'] == TEST_PREDICTOR_NAME
        assert res.json()['is_custom'] is True

    def test_8_delete_from_http_api(self):
        res = requests.delete(f'{root}/predictors/{TEST_PREDICTOR_NAME}')
        assert res.status_code == 200
        res = requests.get(f'{root}/predictors')
        assert TEST_PREDICTOR_NAME not in [x['name'] for x in res.json()]


if __name__ == "__main__":
    unittest.main(failfast=True)
