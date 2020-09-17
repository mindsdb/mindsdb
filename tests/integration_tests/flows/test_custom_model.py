import unittest
import requests
import csv
import inspect
import shutil
import requests
import os
import time

from mindsdb.utilities.config import Config

from common import (
    run_environment,
    get_test_csv,
    TEST_CONFIG
)


root = 'http://127.0.0.1:47334'
TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

EXTERNAL_DS_NAME = 'test_external'
config = Config(TEST_CONFIG)
PRED_NAME = 'this_is_my_custom_sklearnmodel'

def query(query):
    if 'CREATE ' not in query.upper() and 'INSERT ' not in query.upper():
        query += ' FORMAT JSON'

    host = config['integrations']['default_clickhouse']['host']
    port = config['integrations']['default_clickhouse']['port']

    connect_string = f'http://{host}:{port}'

    params = {'user': 'default'}
    try:
        params['user'] = config['integrations']['default_clickhouse']['user']
    except Exception:
        pass

    try:
        params['password'] = config['integrations']['default_clickhouse']['password']
    except Exception:
        pass

    res = requests.post(
        connect_string,
        data=query,
        params=params
    )

    if res.status_code != 200:
        print(f'ERROR: code={res.status_code} msg={res.text}')
        raise Exception()

    if ' FORMAT JSON' in query:
        res = res.json()['data']

    return res


class CustomModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment('clickhouse', config, run_apis=['mysql', 'http'])
        cls.mdb = mdb

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

        query('create database if not exists test')
        test_tables = query('show tables from test')
        test_tables = [x['name'] for x in test_tables]

        test_csv_path = get_test_csv(TEST_CSV['name'], TEST_CSV['url'])

        if TEST_DATA_TABLE not in test_tables:
            print('creating test data table...')
            query(f'''
                CREATE TABLE test.{TEST_DATA_TABLE} (
                    id Int16,
                    number_of_rooms Int8,
                    number_of_bathrooms Int8,
                    sqft Int32,
                    location String,
                    days_on_market Int16,
                    initial_price Int32,
                    neighborhood String,
                    rental_price Int32
                ) ENGINE = MergeTree()
                ORDER BY id
                PARTITION BY location
            ''')

            with open(test_csv_path) as f:
                csvf = csv.reader(f)
                i = 0
                for row in csvf:
                    if i > 0:
                        number_of_rooms = int(row[0])
                        number_of_bathrooms = int(row[1])
                        sqft = int(float(row[2].replace(',', '.')))
                        location = str(row[3])
                        days_on_market = int(row[4])
                        initial_price = int(row[5])
                        neighborhood = str(row[6])
                        rental_price = int(float(row[7]))
                        query(f'''INSERT INTO test.{TEST_DATA_TABLE} VALUES (
                            {i},
                            {number_of_rooms},
                            {number_of_bathrooms},
                            {sqft},
                            '{location}',
                            {days_on_market},
                            {initial_price},
                            '{neighborhood}',
                            {rental_price}
                        )''')
                    i += 1
            print('done')

        ds = datastore.get_datasource(EXTERNAL_DS_NAME)
        if ds is not None:
            datastore.delete_datasource(EXTERNAL_DS_NAME)
        short_csv_file_path = get_test_csv(f'{EXTERNAL_DS_NAME}.csv', TEST_CSV['url'], lines_count=300, rewrite=True)
        datastore.save_datasource(EXTERNAL_DS_NAME, 'file', 'test.csv', short_csv_file_path)

    def test_1_simple_model_upload(self):
        dir_name = 'test_custom_model'

        try:
            os.mkdir(dir_name)
        except:
            pass

        with open(dir_name + '/model.py', 'w') as fp:
            fp.write("""
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

                     """)

        shutil.make_archive(base_name='my_model', format='zip', root_dir=dir_name)

        # Upload the model (new endpoint)
        res = requests.put(f'{root}/predictors/custom/{PRED_NAME}', files=dict(file=open('my_model.zip','rb')))
        print(res.status_code)
        print(res.text)
        assert res.status_code == 200

        # Train the model (new endpoint, just redirects to the /predictors/ endpoint basically)
        params = {
            'data_source_name': EXTERNAL_DS_NAME,
            'to_predict': 'rental_price',
            'kwargs': {}
        }
        res = requests.post(f'{root}/predictors/{PRED_NAME}/learn', json=params)
        print(res.status_code)
        print(res.text)
        assert res.status_code == 200

        # Run a single value prediction
        params = {
            'when': {'initial_price': 5000}
        }
        url = f'{root}/predictors/{PRED_NAME}/predict'
        res = requests.post(url, json=params)
        assert res.status_code == 200
        assert isinstance(res.json()[0]['rental_price']['predicted_value'], float)
        assert 4500 < res.json()[0]['rental_price']['predicted_value'] < 5500

        params = {
            'data_source_name': EXTERNAL_DS_NAME
        }
        url = f'{root}/predictors/{PRED_NAME}/predict_datasource'
        res = requests.post(url, json=params)

        assert res.status_code == 200
        assert(len(res.json()) == 299)
        for pred in res.json():
            assert isinstance(pred['rental_price']['predicted_value'], float)

    def test_2_db_predict_from_external_datasource(self):
        res = query(f"""SELECT rental_price FROM mindsdb.{PRED_NAME} WHERE external_datasource='{EXTERNAL_DS_NAME}'""")

        self.assertTrue(len(res) > 0)
        self.assertTrue(res[0]['rental_price'] is not None and res[0]['rental_price'] != 'None')

    def test_3_retrain_model(self):
        res = query(f""" INSERT INTO mindsdb.predictors (name, predict, select_data_query) VALUES ('{PRED_NAME}', 'sqft', 'SELECT * FROM test.{TEST_DATA_TABLE}') """)
        #sqft

    def test_4_predict_with_retrained_from_sql(self):
        res = query(f"""SELECT sqft FROM mindsdb.{PRED_NAME} WHERE initial_price=6000""")

        self.assertTrue(len(res) > 0)
        print(res)
        self.assertTrue(res[0]['sqft'] is not None and res[0]['sqft'] != 'None')

    def test_5_predict_with_retrained_from_select(self):
        res = query(f"""SELECT sqft FROM mindsdb.{PRED_NAME} WHERE select_data_query='SELECT * FROM test.{TEST_DATA_TABLE}'""")

        self.assertTrue(len(res) > 0)
        print(res)
        self.assertTrue(res[0]['sqft'] is not None and res[0]['sqft'] != 'None')

    def test_6_predict_with_retrained_from_http_api(self):
        params = {
            'when': {'initial_price': 5000, 'rental_price': 4000}
        }
        url = f'{root}/predictors/{PRED_NAME}/predict'
        res = requests.post(url, json=params)
        assert res.status_code == 200
        assert isinstance(res.json()[0]['sqft']['predicted_value'], float)

        params = {
            'data_source_name': EXTERNAL_DS_NAME
        }
        url = f'{root}/predictors/{PRED_NAME}/predict_datasource'
        res = requests.post(url, json=params)

        assert res.status_code == 200
        assert(len(res.json()) == 299)
        for pred in res.json():
            assert isinstance(pred['sqft']['predicted_value'], float)

    def test_7_utils_from_http_api(self):
        res = requests.get(f'{root}/predictors')
        assert res.status_code == 200
        assert PRED_NAME in [x['name'] for x in res.json()]
        for ele in res.json():
            print(ele)
            if ele['name'] == PRED_NAME:
                assert ele['is_custom'] == True

        res = requests.get(f'{root}/predictors/{PRED_NAME}')
        assert res.status_code == 200
        assert res.json()['name'] == PRED_NAME
        assert res.json()['is_custom'] == True

    def test_8_delete_from_http_api(self):
        res = requests.delete(f'{root}/predictors/{PRED_NAME}')
        assert res.status_code == 200
        res = requests.get(f'{root}/predictors')
        assert PRED_NAME not in [x['name'] for x in res.json()]

if __name__ == "__main__":
    unittest.main(failfast=True)
