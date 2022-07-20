import time
import tempfile
import unittest
import json
import uuid

import requests
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
CONTROL_STREAM = f"{INTEGRATION_NAME}_{STREAM_SUFFIX}"
STREAM_IN = f"test_stream_in_{STREAM_SUFFIX}"
STREAM_OUT = f"test_stream_out_{STREAM_SUFFIX}"
STREAM_IN_TS = f"test_stream_in_ts_{STREAM_SUFFIX}"
STREAM_OUT_TS = f"test_stream_out_ts_{STREAM_SUFFIX}"
LEARNING_STREAM = f"test_learning_stream_{STREAM_SUFFIX}"
LEARNING_STREAM_TS = f"test_learning_stream_ts_{STREAM_SUFFIX}"
STREAM_IN_NATIVE = STREAM_IN_TS + "_native"
STREAM_OUT_NATIVE = STREAM_OUT_TS + "_native"
TS_STREAM_IN_NATIVE = 'TS_' + STREAM_IN_NATIVE
TS_STREAM_OUT_NATIVE = 'TS_' + STREAM_OUT_NATIVE
STREAM_IN_OL = f"test_stream_in_ol_{STREAM_SUFFIX}"
STREAM_OUT_OL = f"test_stream_out_ol_{STREAM_SUFFIX}"
DEFAULT_PREDICTOR = "redis_predictor"
TS_PREDICTOR = "redis_ts_predictor"
DS_NAME = "redis_test_ds"
TS_DS_NAME = "ts_redis_test_ds"
NORMAL_STREAM_NAME = f'normal_stream_{STREAM_SUFFIX}'


class RedisTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(apis=['http', ])

    def test_length(self):
        print(f"\nExecuting {self._testMethodName}")
        from mindsdb_streams import RedisStream

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

    def upload_file(self, name):
        df = pd.DataFrame({
            'group': ["A" for _ in range(100, 210)] + ["B" for _ in range(100, 210)],
            'order': [x for x in range(100, 210)] + [x for x in range(200, 310)],
            'x1': [x for x in range(100, 210)] + [x for x in range(100, 210)],
            'x2': [x * 2 for x in range(100, 210)] + [x * 3 for x in range(100, 210)],
            'y': [x * 3 for x in range(100, 210)] + [x * 2 for x in range(100, 210)]
        })
        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False) as f:
            df.to_csv(f, index=False)
            f.flush()
            url = f'{HTTP_API_ROOT}/files/{name}'
            data = {
                "file": (f.name, f, 'text/csv')
            }
            res = requests.put(url, files=data)
            res.raise_for_status()

    def train_predictor(self, ds_name, predictor_name):
        params = {
            'integration': 'files',
            'query': f'select * from {ds_name}',
            'to_predict': 'y',
            'kwargs': {
                'time_aim': 20,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/{predictor_name}'
        res = requests.put(url, json=params)
        res.raise_for_status()

    def train_ts_predictor(self, ds_name, predictor_name, with_gb=True):
        ts_settings = {
            "order_by": ["order"],
            "horizon": 1,
            "use_previous_target": True,
            "window": 10}

        if with_gb:
            ts_settings["group_by"] = ["group"]
        else:
            ts_settings["group_by"] = []
        params = {
            'integration': 'files',
            'query': f'select * from {ds_name}',
            # 'data_source_name': ds_name,
            'to_predict': 'y',
            'kwargs': {
                'use_gpu': False,
                'join_learn_process': True,
                'ignore_columns': None,
                'timeseries_settings': ts_settings,
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/{predictor_name}'
        res = requests.put(url, json=params)
        res.raise_for_status()

    def test_1_create_integration(self):
        print(f"\nExecuting {self._testMethodName}")
        url = f'{HTTP_API_ROOT}/config/integrations/{INTEGRATION_NAME}'

        params = {
            "type": "redis",
            "publish": True,
            "connection": CONNECTION_PARAMS,
            "control_stream": CONTROL_STREAM
        }

        res = requests.put(url, json={"params": params})
        self.assertEqual(res.status_code, 200)

    def test_2_create_redis_stream(self):
        print(f"\nExecuting {self._testMethodName}")
        self.upload_file(DS_NAME)
        self.train_predictor(DS_NAME, DEFAULT_PREDICTOR)

        url = f'{HTTP_API_ROOT}/streams/{NORMAL_STREAM_NAME}'
        res = requests.put(url, json={
            "predictor": DEFAULT_PREDICTOR,
            "stream_in": STREAM_IN,
            "stream_out": STREAM_OUT,
            "integration": INTEGRATION_NAME
        })

        self.assertEqual(res.status_code, 200)

    def test_3_making_stream_prediction(self):
        print(f"\nExecuting {self._testMethodName}")
        from mindsdb_streams import RedisStream

        stream_in = RedisStream(STREAM_IN, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT, CONNECTION_PARAMS)

        for x in range(1, 3):
            stream_in.write({'x1': x, 'x2': 2 * x})
        time.sleep(10)

        self.assertEqual(len(list(stream_out.read())), 2)

    def test_4_create_redis_ts_stream(self):
        print(f"\nExecuting {self._testMethodName}")
        self.upload_file(TS_DS_NAME)
        self.train_ts_predictor(TS_DS_NAME, TS_PREDICTOR)

        url = f'{HTTP_API_ROOT}/streams/ts_stream_{STREAM_SUFFIX}'
        res = requests.put(url, json={
            "predictor": TS_PREDICTOR,
            "stream_in": STREAM_IN_TS,
            "stream_out": STREAM_OUT_TS,
            "integration": INTEGRATION_NAME,
        })

        self.assertEqual(res.status_code, 200)

    def test_5_making_ts_stream_prediction(self):
        print(f"\nExecuting {self._testMethodName}")
        from mindsdb_streams import RedisStream
        stream_in = RedisStream(STREAM_IN_TS, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT_TS, CONNECTION_PARAMS)

        for x in range(230, 241):
            stream_in.write({'x1': x, 'x2': 2 * x, 'order': x, 'group': "A", 'y': 3 * x})
            time.sleep(0.01)
        time.sleep(10)

        self.assertEqual(len(list(stream_out.read())), 2)

    def test_6_create_stream_redis_native_api(self):
        print(f"\nExecuting {self._testMethodName}")
        from mindsdb_streams import RedisStream
        control_stream = RedisStream(CONTROL_STREAM, CONNECTION_PARAMS)
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
            stream_in.write({'x1': x, 'x2': 2 * x})
            time.sleep(5)
        time.sleep(30)

        self.assertEqual(len(list(stream_out.read())), 2)

    def test_8_test_online_learning(self):
        print(f"\nExecuting {self._testMethodName}")
        from mindsdb_streams import RedisStream
        control_stream = RedisStream(CONTROL_STREAM, CONNECTION_PARAMS)
        stream_in = RedisStream(STREAM_IN_OL, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT_OL, CONNECTION_PARAMS)
        PREDICTOR_NAME = "ONLINE_LEARNING"

        control_stream.write({
            'action': 'create',
            'name': f'{self._testMethodName}_{STREAM_SUFFIX}',
            'predictor': PREDICTOR_NAME,
            'learning_params': {"to_predict": "y",
                                'kwargs': {
                                    'time_aim': 3}
                                },
            'learning_threshold': 10,
            'stream_in': STREAM_IN_OL,
            'stream_out': STREAM_OUT_OL,
        })

        for x in range(1, 101):
            stream_in.write({'x1': x, 'x2': 2 * x, 'y': 3 * x})

        start_time = time.time()
        while (time.time() - start_time) < 300:
            time.sleep(5)
            res = list(stream_out.read())
            if res and res[0]['status'] == 'success':
                break
        else:
            raise Exception('Create predictor timeout')

        url = f'{HTTP_API_ROOT}/predictors/{PREDICTOR_NAME}'
        res = requests.get(url)
        self.assertEqual(res.status_code, 200,
                         f"expected to get {PREDICTOR_NAME} info, but have {res.text}")

    def test_9_making_ts_stream_prediction_no_group(self):
        print(f"\nExecuting {self._testMethodName}")
        from mindsdb_streams import RedisStream
        PREDICTOR_NAME = TS_PREDICTOR + "_no_group"
        STREAM_IN = STREAM_IN_TS + 'no_group'
        STREAM_OUT = STREAM_OUT_TS + 'no_group'

        self.train_ts_predictor(TS_DS_NAME, PREDICTOR_NAME, with_gb=False)

        url = f'{HTTP_API_ROOT}/streams/ts_stream_{STREAM_SUFFIX}_no_group'
        res = requests.put(url, json={
            "predictor": PREDICTOR_NAME,
            "stream_in": STREAM_IN,
            "stream_out": STREAM_OUT,
            "integration": INTEGRATION_NAME,
        })

        self.assertEqual(res.status_code, 200)
        stream_in = RedisStream(STREAM_IN, CONNECTION_PARAMS)
        stream_out = RedisStream(STREAM_OUT, CONNECTION_PARAMS)

        for x in range(210, 221):
            stream_in.write({'x1': x, 'x2': 2*x, 'order': x, 'y': 3 * x})
            time.sleep(5)

        self.assertEqual(len(list(stream_out.read())), 2)

    def test_delete_stream_http_api(self):
        print(f"\nExecuting {self._testMethodName}")
        url = f'{HTTP_API_ROOT}/streams/{NORMAL_STREAM_NAME}'
        res = requests.delete(url)
        self.assertEqual(res.status_code,
                         200,
                         f"fail to delete {NORMAL_STREAM_NAME} stream: {res.text}")

        res = requests.get(url)
        self.assertNotEqual(res.status_code, 200, f"stream {NORMAL_STREAM_NAME} still exist")


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
