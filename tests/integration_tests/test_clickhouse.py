import unittest
import requests
import os
import csv
import time
import inspect
import subprocess

import MySQLdb

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.utilities.config import Config


TEST_CONFIG = '/home/maxs/dev/mdb/venv/sources/mindsdb/test_config.json'

test_csv = 'tests/home_rentals.csv'
test_data_table = 'home_rentals_400'
test_predictor_name = 'test_predictor_400'

config = Config(TEST_CONFIG)

def query_ch(query):
    if 'CREATE ' not in query.upper() and 'INSERT ' not in query.upper():
        query += ' FORMAT JSON'

    user = config['integrations']['clickhouse']['user']
    password = config['integrations']['clickhouse']['password']

    connect_string = 'http://{}:{}'.format(
        'localhost',
        8123
    )

    params = {}

    params = {'user': 'default'}
    try:
        params['user'] = config['integrations']['clickhouse']['user']
    except:
        pass

    try:
        params['password'] = config['integrations']['clickhouse']['password']
    except:
        pass

    res = requests.post(
        connect_string,
        data=query,
        params=params
    )

    if ' FORMAT JSON' in query:
        res = res.json()['data']

    return res

class ClickhouseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mdb = MindsdbNative(config)

        if os.path.isfile(test_csv) is False:
            r = requests.get("https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv")
            with open(test_csv, 'wb') as f:
                f.write(r.content)

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if test_predictor_name in models:
            cls.mdb.delete_model(test_predictor_name)

        query_ch('create database if not exists test')
        test_tables = query_ch('show tables from test')
        test_tables = [x['name'] for x in test_tables]
        if test_data_table not in test_tables:
            query_ch(f'''
                CREATE TABLE test.{test_data_table} (
                number_of_rooms Int8,
                number_of_bathrooms Int8,
                sqft Int32,
                location String,
                days_on_market Int16,
                initial_price Int32,
                neighborhood String,
                rental_price Int32
                ) ENGINE = TinyLog()
            ''')
            with open(test_csv) as f:
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
                        query_ch(f'''INSERT INTO test.{test_data_table} VALUES (
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

    def test_1_predictor_record_not_exists(self):
        print(f'Executing {inspect.stack()[0].function}')
        # try:
        #     pass
        # except expression as identifier:
        #     pass
        result = query_ch(f"select name from mindsdb.predictors where name='{test_predictor_name}'")
        self.assertTrue(len(result) == 0)
        print('Passed')

    def test_2_predictor_table_not_exists(self):
        print(f'Executing {inspect.stack()[0].function}')
        result = query_ch(f"show tables from mindsdb")
        result = [x['name'] for x in result]
        self.assertTrue(test_predictor_name not in result)
        print('Passed')

    def test_3_learn_predictor(self):
        print('Executing test 3')
        q = f"""
            insert into mindsdb.predictors
                (name, predict_cols, select_data_query, training_options)
            values (
                '{test_predictor_name}',
                'rental_price',
                'select * from test.{test_data_table} limit 1000',
                '{{"stop_training_in_x_seconds": 30}}'
            )
        """
        result = query_ch(q)

        time.sleep(40)

        result = query_ch(f"select name from mindsdb.predictors where name='{test_predictor_name}'")
        # check status!
        self.assertTrue(len(result) == 1)

        result = query_ch(f"show tables from mindsdb")
        result = [x['name'] for x in result]
        self.assertTrue(test_predictor_name in result)

    def test_4_query(self):
        print('Executing test 4')
        result = query_ch(f"select rental_price from mindsdb.{test_predictor_name} where sqft=1000 and location='good'")
        self.assertTrue(len(result) == 1 and 'rental_price' in result[0])

def wait_mysql(timeout):
    config

        
    con = MySQLdb.connect(
        config['api']['mysql']['host'],
        USER,
        PASSWORD,
        DATABASE
    )

    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS test_mindsdb')
    cur.execute('CREATE TABLE test_mindsdb(col_1 Text, col_2 BIGINT, col_3 BOOL)')
    for i in range(0,200):
        cur.execute(f'INSERT INTO test_mindsdb VALUES ("This is tring number {i}", {i}, {i % 2 == 0})')
    con.commit()
    con.close()

if __name__ == "__main__":
    sp = subprocess.Popen(['python3', '-m', 'mindsdb', '--api', 'mysql', '--config', TEST_CONFIG])
    try:
        time.sleep(12)
        unittest.main()
        print('Tests passed !')
    except:
        print('Tests Failed !')
    finally:
        sp.terminate()
    print('done')