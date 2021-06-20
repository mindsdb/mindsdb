import time
import tempfile
import unittest
import json
import uuid

import requests
import pandas as pd

from common import HTTP_API_ROOT, run_environment, EXTERNAL_DB_CREDENTIALS, USE_EXTERNAL_DB_SERVER

from mindsdb.streams import RedisStream

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

    def test_length(self):
        stream = RedisStream(f'test_stream_length_{STREAM_SUFFIX}', CONNECTION_PARAMS)

        self.assertEqual(len(list(stream.read())), 0)
        time.sleep(5)

        stream.write({'0': 0})
        time.sleep(5)

        self.assertEqual(len(list(stream.read())), 1)

        stream.write({'0': 0})
        stream.write({'0': 0})
        time.sleep(5)

        self.assertEqual(len(list(stream.read())), 2)
        self.assertEqual(len(list(stream.read())), 0)

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
        params = {"type": "redis", "connection": CONNECTION_PARAMS}
        res = requests.put(url, json={"params": params})
        self.assertEqual(res.status_code, 200)

    def test_2_create_redis_stream(self):
        self.upload_ds(DS_NAME)
        self.train_predictor(DS_NAME, DEFAULT_PREDICTOR)

        url = f'{HTTP_API_ROOT}/streams/{self._testMethodName}_{STREAM_SUFFIX}'
        res = requests.put(url, json={
            "predictor": DEFAULT_PREDICTOR,
            "stream_in": STREAM_IN,
            "stream_out": STREAM_OUT,
            "integration": INTEGRATION_NAME
        })

        self.assertEqual(res.status_code, 200)

    def test_3_making_stream_prediction(self):
        stream_in = RedisStream(STREAM_IN, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT, CONNECTION_PARAMS)

        for x in range(1, 3):
            stream_in.write({'x1': x, 'x2': 2*x})
            time.sleep(5)

        self.assertEqual(len(list(stream_out.read())), 2)

    def test_4_create_redis_ts_stream(self):
        self.train_ts_predictor(DS_NAME, TS_PREDICTOR)

        url = f'{HTTP_API_ROOT}/streams/{self._testMethodName}_{STREAM_SUFFIX}'
        res = requests.put(url, json={
            "predictor": TS_PREDICTOR,
            "stream_in": STREAM_IN_TS,
            "stream_out": STREAM_OUT_TS,
            "integration": INTEGRATION_NAME,
        })

        self.assertEqual(res.status_code, 200)

    def test_5_making_ts_stream_prediction(self):
        stream_in = RedisStream(STREAM_IN_TS, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT_TS, CONNECTION_PARAMS)

        for x in range(210, 221):
            stream_in.write({'x1': x, 'x2': 2*x, 'order': x, 'group': "A"})
            time.sleep(5)

        self.assertEqual(len(list(stream_out.read())), 2)

    def test_6_create_stream_redis_native_api(self):
        STREAM_IN_NATIVE = STREAM_IN + "_native"
        STREAM_OUT_NATIVE = STREAM_OUT + "_native"

        control_stream = RedisStream('control_stream_' + INTEGRATION_NAME, CONNECTION_PARAMS)
        control_stream.write({
            'action': 'create',
            'name': f'{self._testMethodName}_{STREAM_SUFFIX}',
            'predictor': DEFAULT_PREDICTOR,
            'stream_in': STREAM_IN_NATIVE,
            'stream_out': STREAM_OUT_NATIVE,
        })

        time.sleep(5)

        stream_in = RedisStream(STREAM_IN_NATIVE, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT_NATIVE, CONNECTION_PARAMS)

        for x in range(1, 3):
            stream_in.write({'x1': x, 'x2': 2*x})
            time.sleep(5)

        self.assertEqual(len(list(stream_out.read())), 2)

        control_stream.write({
            'action': 'delete',
            'name': f'{self._testMethodName}_{STREAM_SUFFIX}'
        })

        time.sleep(5)

        for x in range(1, 3):
            stream_in.write({'x1': x, 'x2': 2*x})
            time.sleep(5)

        self.assertEqual(len(list(stream_out.read())), 0)

    def test_7_create_ts_stream_redis_native_api(self):
        STREAM_IN_NATIVE = STREAM_IN_TS + "_native"
        STREAM_OUT_NATIVE = STREAM_OUT_TS + "_native"

        control_stream = RedisStream('control_stream_' + INTEGRATION_NAME, CONNECTION_PARAMS)
        control_stream.write({
            'action': 'create',
            'name': f'{self._testMethodName}_{STREAM_SUFFIX}',
            'predictor': TS_PREDICTOR,
            'stream_in': STREAM_IN_NATIVE,
            'stream_out': STREAM_OUT_NATIVE,
        })

        time.sleep(5)

        stream_in = RedisStream(STREAM_IN_NATIVE, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT_NATIVE, CONNECTION_PARAMS)

        for x in range(210, 221):
            stream_in.write({'x1': x, 'x2': 2*x, 'order': x, 'group': "A"})
            time.sleep(5)

        self.assertEqual(len(list(stream_out.read())), 2)

        control_stream.write({
            'action': 'delete',
            'name': f'{self._testMethodName}_{STREAM_SUFFIX}'
        })

        for x in range(210, 221):
            stream_in.write({'x1': x, 'x2': 2*x, 'order': x, 'group': "A"})
            time.sleep(5)

        self.assertEqual(len(list(stream_out.read())), 0)

if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
