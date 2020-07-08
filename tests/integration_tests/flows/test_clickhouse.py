from subprocess import Popen
import time
import os
import signal
import random
import string
import requests

import unittest

from mindsdb.utilities.config import Config
import mindsdb

rand = ''.join(random.choice(string.ascii_uppercase) for _ in range(11))
ds_name = f'default.hr_ds_{rand}'
pred_name = f'hr_predictor_{rand}'

# Can't be a fixture since it's used in setup/teardown
root = 'http://localhost:47334'

def set_get_config_path():
    os.environ['DEV_CONFIG_PATH'] = 'config'
    return os.environ['DEV_CONFIG_PATH'] + '/config.json'

def query_ch(query, database='default'):
    config = Config(set_get_config_path())
    add = ' FORMAT JSON'
    for ele in ['drop ', 'create ','insert ', 'show ']:
        if ele in query.lower():
            add = ''
    query += add

    connect_string = 'http://{}:{}'.format(
        config['integrations']['default_clickhouse']['host'],
        config['integrations']['default_clickhouse']['port']
    )

    params = {'user': config['integrations']['default_clickhouse']['user'], 'password': config['integrations']['default_clickhouse']['password'], 'database': database}

    res = requests.post(
        connect_string,
        data=query,
        params=params
    )

    if res.status_code != 200:
        print(f'Error in query: {query}')

    if ' FORMAT JSON' in query:
        return res.json()['data']
    else:
        return res.text

class ClickhouseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        set_get_config_path()

        cls.sp = Popen(['python3', '-m', 'mindsdb'], close_fds=True)

        for i in range(20):
            try:
                res = requests.get(f'{root}/util/ping')
                if res.status_code != 200:
                    raise Exception('')
            except:
                time.sleep(1)
                if i == 19:
                    raise Exception("Can't connect !")

        #query_ch('DROP DATABASE mindsdb')
        query_ch(f"""
        CREATE TABLE {ds_name} (number_of_rooms String, number_of_bathrooms String, sqft Int64, location String, days_on_market Int64, initial_price Int64, neighborhood String, rental_price Float64)  ENGINE=URL('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/home_rentals/dataset/train.csv', CSVWithNames)
        """)

    @classmethod
    def tearDownClass(cls):
        try:
            pgrp = os.getpgid(cls.sp.pid)
            os.killpg(pgrp, signal.SIGINT)
            os.remove(set_get_config_path())
            os.system('fuser -k 47335/tcp ; fuser -k 47334/tcp')
        except:
            pass

    def test_1_setup(self):
        result = query_ch(f"show tables", 'mindsdb')
        assert 'predictors' in result
        assert 'commands' in result

    def test_2_learn(self):
        q = f"""
            insert into mindsdb.predictors
                (name, predict_cols, select_data_query, training_options)
            values (
                '{pred_name}',
                'rental_price',
                'SELECT * FROM {ds_name} LIMIT 400',
                '{{"stop_training_in_x_seconds": 10}}'
            )
        """
        result = query_ch(q)
        time.sleep(40)
        for i in range(40):
            try:
                result = query_ch(f"SELECT name FROM mindsdb.predictors where name='{pred_name}'")
                if result[0]['name'] != pred_name:
                    raise Exception('not ready yet !')
            except:
                time.sleep(1)
                if i == 39:
                    raise Exception("Can't get predictor !")

        result = query_ch(f"show tables", 'mindsdb')
        assert pred_name in result

    def test_3_predict_from_where(self):
        result = query_ch(f"SELECT rental_price FROM mindsdb.{pred_name} where sqft=1000 and location='good'")
        assert len(result) == 1
        assert 'rental_price' in result[0]

    def test_4_predict_from_query(self):
        len_ds = query_ch(f'SELECT COUNT(*) as len from {ds_name}')[0]['len']
        result = query_ch(f""" SELECT rental_price FROM mindsdb.{pred_name} where select_data_query=='SELECT * FROM {ds_name}' """)
        print(len(result), len_ds)
        assert int(len(result)) == int(len_ds)
        for res in result:
            assert 'rental_price' in res
            #assert 'rental_price_explain' in res
            #assert 'rental_price_confidence' in res
            #assert 'rental_price_max' in res
            #assert 'rental_price_min' in res

if __name__ == '__main__':
    unittest.main()
