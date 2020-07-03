import unittest
import requests
import os
import csv
import time
import inspect
import subprocess
import pathlib
import atexit
import json

import mysql.connector

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.utilities.config import Config
from mindsdb.interfaces.database.database import DatabaseWrapper

from common import wait_port, prepare_config

TEST_CONFIG = '/path_to/config.json'

START_TIMEOUT = 15

TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

config = Config(TEST_CONFIG)

def query(q, as_dict=False):
    con = mysql.connector.connect(
        host=config['integrations']['default_mariadb']['host'],
        port=config['integrations']['default_mariadb']['port'],
        user=config['integrations']['default_mariadb']['user'],
        passwd=config['integrations']['default_mariadb']['password'],
        db='mindsdb'
    )

    cur = con.cursor(dictionary=as_dict)
    cur.execute(q)
    res = True
    try:
        res = cur.fetchall()
    except:
        pass
    con.commit()
    con.close()
    return res

def create_churn_dataset(self):
    for mode in ['train','test']:
        os.system(f'rm {test_csv}')
        cls.mdb = MindsdbNative(config)

        if os.path.isfile(test_csv) is False:
            r = requests.get(f"https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/churn/dataset/{mode}.csv")
            with open(test_csv, 'wb') as f:
                f.write(r.content)

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

        query('create database if not exists test')
        test_tables = query('show tables from test')
        test_tables = [x[0] for x in test_tables]
        if TEST_DATA_TABLE not in test_tables:
            query(f'DROP TABLE IF EXISTS data.{TEST_DATA_TABLE}_{mode}')
            query(f'''
                CREATE TABLE data.{TEST_DATA_TABLE}_{mode} (
                    CreditScore int,
                    Geography varchar(300),
                    Gender varchar(300),
                    Age int,
                    Tenure int,
                    Balance float,
                    NumOfProducts int,
                    HasCrCard int,
                    IsActiveMember int,
                    EstimatedSalary float,
                    Exited int
                )
            ''')
            with open(test_csv) as f:
                csvf = csv.reader(f)
                i = 0
                for row in csvf:
                    if i > 0:
                        CreditScore = int(row[0])
                        Geography = str(row[1])
                        Gender = str(row[2])
                        Age = int(row[3])
                        Tenure = int(row[4])
                        Balance = float(row[5])
                        NumOfProducts = int(row[6])
                        HasCrCard = int(row[7])
                        IsActiveMember = int(row[8])
                        EstimatedSalary = float(row[9])
                        Exited = int(row[10])

                        query(f'''INSERT INTO data.{TEST_DATA_TABLE}_{mode} VALUES (
                            {CreditScore},
                            '{Geography}',
                            '{Gender}',
                            {Age},
                            {Tenure},
                            {Balance},
                            {NumOfProducts},
                            {HasCrCard},
                            {IsActiveMember},
                            {EstimatedSalary},
                            {Exited}
                        )''')
                    i += 1
    os.system(f'rm {test_csv}')

class MariaDBTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mdb = MindsdbNative(config)

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

        query('create database if not exists test')
        test_tables = query('show tables from test')
        test_tables = [x[0] for x in test_tables]
        if TEST_DATA_TABLE not in test_tables:
            print('creating test data table...')
            query(f'''
                CREATE TABLE test.{TEST_DATA_TABLE} (
                    number_of_rooms int,
                    number_of_bathrooms int,
                    sqft int,
                    location varchar(100),
                    days_on_market int,
                    initial_price int,
                    neighborhood varchar(100),
                    rental_price int
                )
            ''')

            test_csv_path = str(pathlib.Path(__file__).parent.absolute().joinpath('../temp/', TEST_CSV['name']).resolve())
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
        test_tables = [x[0] for x in test_tables]
        self.assertTrue(TEST_DATA_TABLE in test_tables)

        print(f'Test predictor table not exists')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x[0] for x in mindsdb_tables]
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
        res = query(f"select status from mindsdb.predictors where name = '{TEST_PREDICTOR_NAME}'", as_dict=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print(f'predictor table in mindsdb db')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x[0] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME in mindsdb_tables)

    def test_3_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = query(f"""
            select
                rental_price, location, sqft, $rental_price_confidence, number_of_rooms
            from
                mindsdb.{TEST_PREDICTOR_NAME} where sqft=1000;
        """, as_dict=True)

        print('check result')
        self.assertTrue(len(res) == 1)

        res = res[0]

        self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
        self.assertTrue(res['location'] is not None and res['location'] != 'None')
        self.assertTrue(res['sqft'] == 1000)
        self.assertIsInstance(res['$rental_price_confidence'], float)
        self.assertTrue(res['number_of_rooms'] == 'None' or res['number_of_rooms'] is None)

    def test_4_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        results = query(f"""
            select
                rental_price, location, sqft, $rental_price_confidence, number_of_rooms
            from
                mindsdb.{TEST_PREDICTOR_NAME} where select_data_query='select * from test.{TEST_DATA_TABLE} limit 3';
        """, as_dict=True)

        print('check result')
        self.assertTrue(len(results) == 3)
        for res in results:
            self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
            self.assertTrue(res['location'] is not None and res['location'] != 'None')
            self.assertIsInstance(res['$rental_price_confidence'], float)

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
        mindsdb_tables = [x[0] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)

    def test_6_insert_predictor_again(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        self.test_2_insert_predictor()

    def test_7_delete_predictor_by_delete_statement(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            delete from mindsdb.predictors where name='{TEST_PREDICTOR_NAME}';
        """)

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print(f'Test predictor table not exists')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x[0] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)


def wait_mysql(timeout):
    global config
    m = DatabaseWrapper(config)

    start_time = time.time()

    connected = m.check_connections()['default_mariadb']
    while not connected and (time.time() - start_time) < timeout:
        time.sleep(2)
        connected = m.check_connections()['default_mariadb']

    return connected

def stop_mariadb():
    maria_sp = subprocess.Popen(
        ['./cli.sh', 'mariadb-stop'],
        cwd=pathlib.Path(__file__).parent.absolute().joinpath('../docker/').resolve(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    maria_sp.wait()

if __name__ == "__main__":
    temp_config_path = prepare_config(config)

    maria_sp = subprocess.Popen(
        ['./cli.sh', 'mariadb'],
        cwd=pathlib.Path(__file__).parent.absolute().joinpath('../docker/').resolve(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    atexit.register(stop_mariadb)
    maria_ready = wait_mysql(START_TIMEOUT)

    if maria_ready:
        sp = subprocess.Popen(
            ['python3', '-m', 'mindsdb', '--api', 'mysql', '--config', temp_config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        atexit.register(sp.kill)
        port_num = config['api']['mysql']['port']
    api_ready = maria_ready and wait_port(port_num, START_TIMEOUT)

    try:
        if maria_ready is False or api_ready is False:
            print(f'Failed by timeout. MariaDB started={maria_ready}, MindsDB started={api_ready}')
            raise Exception()
        unittest.main(failfast=True)
        print('Tests passed !')
    except Exception as e:
        print('Tests Failed !')
        print(e)
    print('done')
