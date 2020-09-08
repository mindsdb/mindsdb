import unittest
import requests
import csv
import inspect
import shutil
import requests

from mindsdb.utilities.config import Config

from common import (
    run_environment,
    get_test_csv,
    TEST_CONFIG
)


root = 'http://localhost:47334'
TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

EXTERNAL_DS_NAME = 'test_external'
config = Config(TEST_CONFIG)


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


class ClickhouseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment('clickhouse', config, run_apis='api')
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
        dir = 'test_custom_model'
        zip = 'my_model.zip'
        p_name = 'this_is_my_custom_sklearnmodel'

        os.mkdir(dir)
        with open(dir + '/model.py', 'w') as fp:
            fp.write("""
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd


class Model():
    def setup(self):
        self.model = LinearRegression()

    def get_x(data):
        initial_price = np.array(from_data['initial_price'])
        initial_price.reshpae(-1, 1)
        return initial_price

    def get_y(data, to_predict_str):
        to_predict = np.array(from_data[to_predict])
        return to_predict

    def predict(self, from_data, kwargs):
        initial_price = get_x(from_data)
        rental_price = self.model.predict(initial_price)
        return pd.DataFrame({'rental_price': rental_price})

    def fit(self, from_data, to_predict, data_analysis, kwargs):
        Y = get_y(from_data, to_predict)
        X = get_x(from_data)
        self.model.fit(X, Y)

                     """)

    shutil.make_archive(zip, 'zip', dir)

    # Upload the model (new endpoint)
    res = requests.put(f'{root}/predictors/custom/{p_name}', files=dict(model=open(zip,'rb')))
    assert res.status_code == 200

    # Train the model (new endpoint, just redirects to the /predictors/ endpoint basically)
    params = {
        'data_source_name': EXTERNAL_DS_NAME,
        'to_predict': 'rental_price',
        'kwargs': {}
    }
    res = requests.put(f'{root}/predictors/{p_name}/fit', json=params)

    # Run a single value prediction
    params = {
        'when': {'initial_price': 5000}
    }
    url = f'{root}/predictors/{pred_name}/predict'
    res = requests.post(url, json=params)
    assert res.status_code == 200
    assert isinstance(res.json()[0]['rental_price']['predicted_value'], float)
    assert 4500 < res.json()[0]['rental_price']['predicted_value'] < 5500

    params = {
        'data_source_name': EXTERNAL_DS_NAME
    }
    url = f'{root}/predictors/{pred_name}/predict_datasource'
    res = requests.post(url, json=params)

    assert res.status_code == 200
    assert(len(res.json()) == 300)
    for pred in res.json()[0]:
        assert isinstance(pred['rental_price']['predicted_value'], float)

    def test_2_predict_from_db(self):
        pass

    def test_3_retrain_model(self):
        pass

    def test_4_upload_pretrain_model(self):
        pass



if __name__ == "__main__":
    unittest.main(failfast=True)
