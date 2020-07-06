import pytest
import requests
import runpy

from subprocess import Popen
import time
import os
import signal
from random import randint


root = 'http://localhost:47334'
class TestPredictor:
    @classmethod
    def setup_class(cls):
        cls.sp = Popen(['python3', '-m', 'mindsdb', '--api', 'http'], close_fds=True)

        for i in range(20):
            try:
                res = requests.get(f'{root}/ping')
                print(res)
                if res.status != 200:
                    raise Exception('')
            except:
                time.sleep(1)

    @classmethod
    def teardown_class(cls):
        try:
            pgrp = os.getpgid(cls.sp.pid)
            os.killpg(pgrp, signal.SIGINT)
        except:
            pass

    def test_put_ds_put_pred(self):
        rand = randint(0,pow(10,12))
        PRED_NAME = f'hr_predictor_{rand}'
        DS_NAME = f'hr_ds_{rand}'

        DS_URL = 'https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/home_rentals/dataset/train.csv'

        # PUT datasource
        params = {
            'name': DS_NAME,
            'source_type': 'url',
            'source': DS_URL
        }
        url = f'{root}/datasources/{DS_NAME}'
        res = requests.put(url, json=params)
        assert res.status_code == 200

        response = requests.get(f'{root}/datasources/{DS_NAME}/analyze')
        assert response.status_code == 200

        # PUT predictor
        params = {
            'data_source_name': DS_NAME,
            'to_predict': 'rental_price',
            'kwargs': {
                'stop_training_in_x_seconds': 5,
                'join_learn_process': True
            }
        }
        url = f'{root}/predictors/{PRED_NAME}'
        res = requests.put(url, json=params)
        assert res.status_code == 200

        # POST predictions
        params = {
            'when': {'sqft':500}
        }
        url = f'{root}/predictors/{PRED_NAME}/predict'
        res = requests.post(url, json=params)
        print(res.json())
        assert isinstance(res.json()[0]['rental_price']['predicted_value'],float)
        assert res.status_code == 200


    def test_datasources(self):
        """
        Call list datasources endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/datasources/')
        assert response.status_code == 200

    def test_datasource_not_found(self):
        """
        Call unexisting datasource
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/datasource/dummy_source')
        assert response.status_code == 404

    def test_ping(self):
        """
        Call utilities ping endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/util/ping')
        assert response.status_code == 200

    def test_predictors(self):
        """
        Call list predictors endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/predictors/')
        assert response.status_code == 200

    def test_predictor_not_found(self):
        """
        Call unexisting predictor
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/predictors/dummy_predictor')
        assert response.status_code == 404
