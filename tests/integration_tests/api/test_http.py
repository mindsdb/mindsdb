from subprocess import Popen
import time
import os
import signal
from random import randint

import pytest
import requests
import runpy


@pytest.fixture(scope="module")
def ds_name():
    rand = randint(0,pow(10,12))
    return f'hr_ds_{rand}'

@pytest.fixture(scope="module")
def pred_name():
    rand = randint(0,pow(10,12))
    return f'hr_predictor_{rand}'

root = 'http://localhost:47334'
class TestPredictor:
    @classmethod
    def setup_class(cls):
        cls.sp = Popen(['python3', '-m', 'mindsdb', '--api', 'http'], close_fds=True)

        for i in range(20):
            try:
                res = requests.get(f'{root}/ping')
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

    @pytest.mark.order1
    def test_put_ds(self, ds_name):
        # PUT datasource
        params = {
            'name': ds_name,
            'source_type': 'url',
            'source': 'https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/home_rentals/dataset/train.csv'
        }
        url = f'{root}/datasources/{ds_name}'
        res = requests.put(url, json=params)
        assert res.status_code == 200

    @pytest.mark.order2
    def test_analyze(self, ds_name):
        response = requests.get(f'{root}/datasources/{ds_name}/analyze')
        assert response.status_code == 200

    @pytest.mark.order2
    def test_put_predictor(self, ds_name, pred_name):
        # PUT predictor
        params = {
            'data_source_name': ds_name,
            'to_predict': 'rental_price',
            'kwargs': {
                'stop_training_in_x_seconds': 5,
                'join_learn_process': True
            }
        }
        url = f'{root}/predictors/{pred_name}'
        res = requests.put(url, json=params)
        assert res.status_code == 200

        # POST predictions
        params = {
            'when': {'sqft':500}
        }
        url = f'{root}/predictors/{pred_name}/predict'
        res = requests.post(url, json=params)
        assert isinstance(res.json()[0]['rental_price']['predicted_value'],float)
        assert res.status_code == 200

    @pytest.mark.order3
    def test_datasources(self):
        """
        Call list datasources endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/datasources/')
        assert response.status_code == 200

    @pytest.mark.order3
    def test_datasource_not_found(self):
        """
        Call unexisting datasource
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/datasource/dummy_source')
        assert response.status_code == 404

    @pytest.mark.order3
    def test_ping(self):
        """
        Call utilities ping endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/util/ping')
        assert response.status_code == 200

    @pytest.mark.order3
    def test_predictors(self):
        """
        Call list predictors endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/predictors/')
        assert response.status_code == 200

    @pytest.mark.order3
    def test_predictor_not_found(self):
        """
        Call unexisting predictor
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/predictors/dummy_predictor')
        assert response.status_code == 404
