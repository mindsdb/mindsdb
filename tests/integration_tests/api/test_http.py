import os
from random import randint
from pathlib import Path
import unittest
import requests
import time

import psutil

from mindsdb.utilities.config import Config

import importlib.util
common_path = Path(__file__).parent.parent.absolute().joinpath('flows/common.py').resolve()
spec = importlib.util.spec_from_file_location("common", str(common_path))
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)

rand = randint(0, pow(10, 12))
ds_name = f'hr_ds_{rand}'
pred_name = f'hr_predictor_{rand}'
root = 'http://localhost:47334/api'


class HTTPTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config(common.TEST_CONFIG)
        cls.initial_integrations_names = list(cls.config['integrations'].keys())

        mdb, datastore = common.run_environment(
            cls.config,
            apis=['http'],
            override_integration_config={
                'default_mariadb': {
                    'enabled': True
                },
                'default_clickhouse': {
                    'enabled': True
                }
            },
            mindsdb_database=common.MINDSDB_DATABASE
        )
        cls.mdb = mdb

    @classmethod
    def tearDownClass(cls):
        try:
            conns = psutil.net_connections()
            pid = [x.pid for x in conns if x.status == 'LISTEN' and x.laddr[1] == 47334 and x.pid is not None]
            if len(pid) > 0:
                os.kill(pid[0], 9)
            cls.sp.kill()
        except Exception:
            pass

    def test_1_config(self):
        res = requests.get(f'{root}/config/integrations')
        assert res.status_code == 200
        integration_names = res.json()
        for integration_name in integration_names['integrations']:
            assert integration_name in self.initial_integrations_names

        test_integration_data = {'enabled': False, 'host': 'test', 'type': 'clickhouse'}
        res = requests.put(f'{root}/config/integrations/test_integration', json={'params': test_integration_data})
        assert res.status_code == 200

        res = requests.get(f'{root}/config/integrations/test_integration')
        assert res.status_code == 200
        test_integration = res.json()
        print(test_integration, len(test_integration))
        assert len(test_integration) == 6

        res = requests.delete(f'{root}/config/integrations/test_integration')
        assert res.status_code == 200

        res = requests.get(f'{root}/config/integrations/test_integration')
        assert res.status_code != 200

        for k in test_integration_data:
            assert test_integration[k] == test_integration_data[k]

        for name in ['default_mariadb', 'default_clickhouse']:
            # Get the original
            res = requests.get(f'{root}/config/integrations/{name}')
            assert res.status_code == 200

            integration = res.json()
            for k in ['enabled', 'host', 'port', 'type', 'user']:
                assert k in integration
                assert integration[k] is not None
            assert integration['password'] is None

            # Modify it
            res = requests.post(
                f'{root}/config/integrations/{name}',
                json={'params': {'user': 'dr.Who'}}
            )

            res = requests.get(f'{root}/config/integrations/{name}')
            assert res.status_code == 200
            modified_integration = res.json()
            assert modified_integration['password'] is None
            assert modified_integration['user'] == 'dr.Who'
            for k in integration:
                if k not in ['password', 'date_last_update', 'user']:
                    assert modified_integration[k] == integration[k]

            # Put the original values back in\
            del integration['password']
            res = requests.post(f'{root}/config/integrations/{name}', json={'params': integration})
            res = requests.get(f'{root}/config/integrations/{name}')
            assert res.status_code == 200
            modified_integration = res.json()
            for k in integration:
                if k != 'date_last_update':
                    assert modified_integration[k] == integration[k]

    def test_2_put_ds(self):
        # PUT datasource
        params = {
            'name': ds_name,
            'source_type': 'url',
            'source': 'https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/dataset/train.csv'
        }
        url = f'{root}/datasources/{ds_name}'
        res = requests.put(url, json=params)
        assert res.status_code == 200

        db_ds_name = ds_name + '_db'
        params = {
            'name': db_ds_name,
            'query': 'SELECT arrayJoin([1,2,3]) as a, arrayJoin([1,2,3,4,5,6,7,8]) as b',
            'integration_id': 'default_clickhouse'
        }

        url = f'{root}/datasources/{db_ds_name}'
        res = requests.put(url, json=params)
        assert res.status_code == 200
        ds_data = res.json()
        assert ds_data['source_type'] == 'default_clickhouse'
        assert ds_data['row_count'] == 3 * 8

    def test_3_analyze(self):
        response = requests.get(f'{root}/datasources/{ds_name}/analyze')
        assert response.status_code == 200

    def test_3_put_predictor(self):
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
            'when': {'sqft': 500}
        }
        url = f'{root}/predictors/{pred_name}/predict'
        res = requests.post(url, json=params)
        assert isinstance(res.json()[0]['rental_price']['predicted_value'], float)
        assert res.status_code == 200

    def test_4_datasources(self):
        """
        Call list datasources endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/datasources/')
        assert response.status_code == 200

    def test_5_datasource_not_found(self):
        """
        Call unexisting datasource
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/datasource/dummy_source')
        assert response.status_code == 404

    def test_6_ping(self):
        """
        Call utilities ping endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/util/ping')
        assert response.status_code == 200

    def test_7_predictors(self):
        """
        Call list predictors endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/predictors/')
        assert response.status_code == 200

    def test_8_predictor_not_found(self):
        """
        Call unexisting predictor
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/predictors/dummy_predictor')
        assert response.status_code == 404

    def test_9_gui_is_served(self):
        """
        GUI downloaded and available
        """
        start_time = time.time()
        index = Path(self.config.paths['static']).joinpath('index.html')
        while index.is_file() is False and (time.time() - start_time) > 30:
            time.sleep(1)
        assert index.is_file()
        response = requests.get('http://localhost:47334/')
        assert response.status_code == 200
        assert response.content.decode().find('<head>') > 0


if __name__ == '__main__':
    unittest.main(failfast=True)
