import time
import tempfile
import unittest
import json
import uuid

import requests
import walrus
import pandas as pd

from common import HTTP_API_ROOT, run_environment, EXTERNAL_DB_CREDENTIALS, USE_EXTERNAL_DB_SERVER


INTEGRATION_NAME = 'test_redis'
redis_creds = {}
if USE_EXTERNAL_DB_SERVER:
    with open(EXTERNAL_DB_CREDENTIALS, 'rt') as f:
        redis_creds = json.loads(f.read())['redis']


REDIS_PORT = redis_creds.get('port', 6379)
REDIS_HOST = redis_creds.get('host', "127.0.0.1")
REDIS_PASSWORD = redis_creds.get('password', None)

CONNECTION_PARAMS = {"host": REDIS_HOST, "port": REDIS_PORT, "db": 0, "password": REDIS_PASSWORD}
STREAM_SUFFIX = uuid.uuid4()
STREAM_IN = f"test_stream_in_{STREAM_SUFFIX}"
STREAM_OUT = f"test_stream_out_{STREAM_SUFFIX}"
STREAM_IN_TS = f"test_stream_in_ts_{STREAM_SUFFIX}"
STREAM_OUT_TS = f"test_stream_out_ts_{STREAM_SUFFIX}"
DEFAULT_PREDICTOR = "redis_predictor"
TS_PREDICTOR = "redis_ts_predictor"
DS_NAME = "redis_test_ds"


class RedisTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(apis=['mysql', 'http'])

    def upload_ds(self, name):
        df = pd.DataFrame({
            'group': ["A" for _ in range(100, 210)],
            'order': [x for x in range(100, 210)],
            'x1': [x for x in range(100,210)],
            'x2': [x*2 for x in range(100,210)],
            'y': [x*3 for x in range(100,210)]
        })
        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False) as f:
            df.to_csv(f, index=False)
            f.flush()
            url = f'{HTTP_API_ROOT}/datasources/{name}'
            data = {
                "source_type": (None, 'file'),
                "file": (f.name, f, 'text/csv'),
                "source": (None, f.name.split('/')[-1]),
                "name": (None, name)
            }
            res = requests.put(url, files=data)
            res.raise_for_status()

    def train_predictor(self, ds_name, predictor_name):
        params = {
            'data_source_name': ds_name,
            'to_predict': 'y',
            'kwargs': {
                'stop_training_in_x_seconds': 20,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/{predictor_name}'
        res = requests.put(url, json=params)
        res.raise_for_status()

    def train_ts_predictor(self, ds_name, predictor_name):
        params = {
            'data_source_name': ds_name,
            'to_predict': 'y',
            'kwargs': {
                # 'stop_training_in_x_seconds': 40,
                'use_gpu': False,
                'join_learn_process': True,
                'ignore_columns': None,
                'timeseries_settings': {
                    "order_by": ["order"],
                    "group_by": ["group"],
                    "nr_predictions": 1,
                    "use_previous_target": True,
                    "window": 10
                },
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/{predictor_name}'
        res = requests.put(url, json=params)
        res.raise_for_status()

    def test_1_create_integration(self):
        url = f'{HTTP_API_ROOT}/config/integrations/{INTEGRATION_NAME}'
        try:
            res = requests.put(url, json={"params": {"type": "redis", "connection": CONNECTION_PARAMS}})
            self.assertTrue(res.status_code == 200, res.text)
        except Exception as e:
            self.fail(e)

    def test_2_create_redis_stream(self):
        try:
            self.upload_ds(DS_NAME)
        except Exception as e:
            self.fail(f"couldn't upload datasource: {e}")

        try:
            self.train_predictor(DS_NAME, DEFAULT_PREDICTOR)
        except Exception as e:
            self.fail(f"couldn't train predictor: {e}")

        try:
            url = f'{HTTP_API_ROOT}/streams/{self._testMethodName}_{STREAM_SUFFIX}'
            res = requests.put(url, json={
                "name": "test_redis_stream",
                "predictor": DEFAULT_PREDICTOR,
                "stream_in": STREAM_IN,
                "stream_out": STREAM_OUT,
                "integration": INTEGRATION_NAME
            })
            self.assertTrue(res.status_code == 200, res.text)
        except Exception as e:
            self.fail(f"error creating stream: {e}")

    def test_3_making_stream_prediction(self):
        client = walrus.Database(**CONNECTION_PARAMS)
        stream_in = client.Stream(STREAM_IN)
        stream_out = client.Stream(STREAM_OUT)

        for x in range(1, 3):
            when_data = {'x1': x, 'x2': 2*x}
            stream_in.add({'': json.dumps(when_data)})

        time.sleep(10)

        predictions = stream_out.read()
        stream_out.trim(0, approximate=False)
        stream_in.trim(0, approximate=False)
        self.assertTrue(len(predictions)==2)

    def test_4_create_redis_ts_stream(self):
        try:
            self.train_ts_predictor(DS_NAME, TS_PREDICTOR)
        except Exception as e:
            self.fail(f"couldn't train ts predictor: {e}")

        try:
            url = f'{HTTP_API_ROOT}/streams/{self._testMethodName}_{STREAM_SUFFIX}'
            res = requests.put(url, json={
                "name": "test_redis_stream_ts",
                "predictor": TS_PREDICTOR,
                "stream_in": STREAM_IN_TS,
                "stream_out": STREAM_OUT_TS,
                "integration": INTEGRATION_NAME,
            })
            self.assertTrue(res.status_code == 200, res.text)
        except Exception as e:
            self.fail(f"error creating stream: {e}")

    def test_5_making_ts_stream_prediction(self):
        client = walrus.Database(**CONNECTION_PARAMS)
        stream_in = client.Stream(STREAM_IN_TS)
        stream_out = client.Stream(STREAM_OUT_TS)

        for x in range(210, 223):
            when_data = {'x1': x, 'x2': 2*x, 'order': x, 'group': "A"}
            stream_in.add({'': json.dumps(when_data)})

        time.sleep(10)

        predictions = stream_out.read()
        stream_out.trim(0, approximate=False)
        stream_in.trim(0, approximate=False)
        self.assertTrue(len(predictions)==2, f"expected 2 predictions, but got {len(predictions)}")


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
