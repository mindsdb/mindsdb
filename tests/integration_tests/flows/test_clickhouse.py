import unittest
import requests
import os
import csv
import time
import inspect
import subprocess
from random import randint
import MySQLdb

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.utilities.config import Config

@pytest.fixture(scope="module")
def ds_name():
    rand = randint(0,pow(10,12))
    return f'default.hr_ds_{rand}'

@pytest.fixture(scope="module")
def pred_name():
    rand = randint(0,pow(10,12))
    return f'hr_predictor_{rand}'

@pytest.fixture(scope="module")
def config_path():
    os.environ['DEV_CONFIG_PATH'] = ''
    return os.environ['DEV_CONFIG_PATH'] + 'config.json'


def query_ch(query, config_path):
    config = Config(config_path)
    if 'CREATE ' not in query.upper() and 'INSERT ' not in query.upper():
        query += ' FORMAT JSON'

    connect_string = 'http://{}:{}'.format(
        config['integrations']['default_clickhouse']['host'],
        config['integrations']['default_clickhouse']['port']
    )

    params = {'user': config['integrations']['default_clickhouse']['user'], 'password': config['integrations']['default_clickhouse']['password']}

    res = requests.post(
        connect_string,
        data=query,
        params=params
    )

    if ' FORMAT JSON' in query:
        res = res.json()['data']

    return res

class ClickhouseTest:
    @classmethod
    def setup_class(cls, config_path):
        query_ch('DROP DATABASE mindsdb')

        query_ch(f"""
        CREATE TABLE {ds_name} (number_of_rooms String, number_of_bathrooms String, sqft Int64, location String, days_on_market Int64, initial_price Int64, neighborhood String, rental_price Float64)  ENGINE=URL('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/home_rentals/dataset/train.csv', CSVWithNames)
        """)

        cls.sp = Popen(['python3', '-m', 'mindsdb', '--api', 'mysql'], close_fds=True)

        for i in range(20):
            try:
                res = requests.get(f'{root}/ping')
                if res.status != 200:
                    raise Exception('')
            except:
                time.sleep(1)

    @classmethod
    def teardown_class(cls, config_path):
        try:
            pgrp = os.getpgid(cls.sp.pid)
            os.killpg(pgrp, signal.SIGINT)
            os.remove(config_path)
        except:
            pass

    @pytest.mark.order1
    def test_setup(self):
        print(f'Executing {inspect.stack()[0].function}')
        result = query_ch(f"show tables FROM mindsdb")
        names = [x['name'] for x in result]
        assert 'predictors' in names
        assert 'commands' in names

    @pytest.mark.order2
    def test_learn(self, ds_name, pred_name):
        print('Executing test 3')
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

        for i in range(40):
            try:
                result = query_ch(f"SELECT name FROM mindsdb.predictors where name='{pred_name}'")
                print(result)
                assert isinstance(result, dict)
            except:
                time.sleep(1)

        assert len(result) == 1

        result = query_ch(f"show tables FROM mindsdb")
        assert pred_name in result

    @pytest.mark.order3
    def test_predict_from_where(self, pred_name):
        result = query_ch(f"SELECT rental_price FROM mindsdb.{pred_name} where sqft=1000 and location='good'")
        assert len(result) == 1
        assert 'rental_price' in result[0]

    @pytest.mark.order3
    def test_predict_from_query(self, pred_name, ds_name):
        len_ds = query_ch(f'SELECT COUNT(*) as len from {ds_name}')[0]['len']
        result = query_ch(f""" SELECT rental_price FROM mindsdb.{pred_name} where `$select_data_query='SELECT * FROM {ds_name}'` """)
        assert len(result) == len_ds
        for res in result:
            assert 'rental_price' in res
            assert 'rental_price_explain' in res
            assert 'rental_price_confidence' in res
            assert 'rental_price_max' in res
            assert 'rental_price_min' in res
