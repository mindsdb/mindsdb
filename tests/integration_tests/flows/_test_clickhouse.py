import unittest
import requests
import os
import csv
import time
import inspect
import subprocess
import atexit

import MySQLdb

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.utilities.config import Config

from common import wait_api_ready, prepare_config, wait_db, TEST_CONFIG, START_TIMEOUT, TESTS_ROOT

TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

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
    except:
        pass

    try:
        params['password'] = config['integrations']['default_clickhouse']['password']
    except:
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
        cls.mdb = MindsdbNative(config)

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

        query('create database if not exists test')
        test_tables = query('show tables from test')
        test_tables = [x['name'] for x in test_tables]
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

            test_csv_path = str(TESTS_ROOT.joinpath('temp/', TEST_CSV['name']).resolve())
            if os.path.isfile(test_csv_path) is False:
                r = requests.get(TEST_CSV['url'])
                with open(test_csv_path, 'wb') as f:
                    f.write(r.content)

            with open(test_csv_path) as f:
                csvf = csv.reader(f)
                i = 0
                for row in csvf:
                    if i > 0:
                        number_of_rooms = int(row[0])
                        number_of_bathrooms = int(row[1])
                        sqft = int(float(row[2].replace(',','.')))
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

    def test_1_initial_state(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        print('Check all testing objects not exists')
        
        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print(f'Test datasource exists')
        test_tables = query('show tables from test')
        test_tables = [x['name'] for x in test_tables]
        self.assertTrue(TEST_DATA_TABLE in test_tables)

        print(f'Test predictor table not exists')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)

        print(f'mindsdb.predictors table exists')
        self.assertTrue('predictors' in mindsdb_tables)

        print(f'mindsdb.commands table exists')
        self.assertTrue('commands' in mindsdb_tables)

    def test_2_insert_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            insert into mindsdb.predictors (name, predict_cols, select_data_query, training_options) values
            (
                '{TEST_PREDICTOR_NAME}',
                'rental_price, location',
                'select * from test.{TEST_DATA_TABLE} limit 100',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        print(f'predictor record in mindsdb.predictors')
        res = query(f"select status from mindsdb.predictors where name = '{TEST_PREDICTOR_NAME}'")
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print(f'predictor table in mindsdb db')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME in mindsdb_tables)


    def test_3_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = query(f"""
            select
                rental_price, location, sqft, number_of_rooms,
                rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
            from
                mindsdb.{TEST_PREDICTOR_NAME} where sqft=1000
        """)

        print('check result')
        self.assertTrue(len(res) == 1)

        res = res[0]

        self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
        self.assertTrue(res['location'] is not None and res['location'] != 'None')
        # NOTE in current Clickhouse all int fields returns as strings
        self.assertTrue(res['sqft'] == '1000')
        self.assertIsInstance(res['rental_price_confidence'], float)
        self.assertIsInstance(res['rental_price_min'], int)
        self.assertIsInstance(res['rental_price_max'], int)
        self.assertIsInstance(res['rental_price_explain'], str)
        self.assertTrue(res['number_of_rooms'] == 'None' or res['number_of_rooms'] is None)

    def test_4_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        results = query(f"""
            select
                rental_price, location, sqft, number_of_rooms,
                rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
            from
                mindsdb.{TEST_PREDICTOR_NAME} where select_data_query='select * from test.{TEST_DATA_TABLE} limit 3'
        """)

        print('check result')
        self.assertTrue(len(results) == 3)
        for res in results:
            self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
            self.assertTrue(res['location'] is not None and res['location'] != 'None')
            self.assertIsInstance(res['rental_price_confidence'], float)
            self.assertIsInstance(res['rental_price_min'], int)
            self.assertIsInstance(res['rental_price_max'], int)
            self.assertIsInstance(res['rental_price_explain'], str)

    def test_5_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            insert into mindsdb.commands values ('delete predictor {TEST_PREDICTOR_NAME}');
        """)

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print(f'Test predictor table not exists')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)


def stop_clickhouse():
    ch_sp = subprocess.Popen(
        ['./cli.sh', 'clickhouse-stop'],
        cwd=TESTS_ROOT.joinpath('docker/').resolve(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    ch_sp.wait()

if __name__ == "__main__":
    temp_config_path = prepare_config(config, 'default_clickhouse')

    subprocess.Popen(
        ['./cli.sh', 'clickhouse'],
        cwd=TESTS_ROOT.joinpath('docker/').resolve(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    atexit.register(stop_clickhouse)
    clickhouse_ready = wait_db(config, 'default_clickhouse')

    if clickhouse_ready:
        sp = subprocess.Popen(
            ['python3', '-m', 'mindsdb', '--api', 'mysql', '--config', temp_config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        atexit.register(sp.kill)

    api_ready = clickhouse_ready and wait_api_ready(config)

    try:
        if api_ready is False:
            print(f'Failed by timeout. ClickHouse started={clickhouse_ready} MindsDB started={api_ready}')
            raise Exception()
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
