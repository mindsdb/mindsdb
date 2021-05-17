import tempfile
import json
import unittest
import requests

import pandas as pd

from common import HTTP_API_ROOT, run_environment

INTEGRATION_NAME = 'test_redis'
REDIS_PORT = 6379


class RedisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        run_environment(apis=['mysql', 'http'])

    def upload_ds(self, name):
        df = pd.DataFrame({
                'z1': [x for x in range(100,110)]
                ,'z2': [x*2 for x in range(100,110)]
            })
        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False) as f:
            print(f"TEMP FILE: {f.name}")
            df.to_csv(f, index=False)
            f.flush()
            url = f'{HTTP_API_ROOT}/datasources/{name}'
            data = {"source_type": (None, 'file'),
                    "file": (f.name, f, 'text/csv'),
                    "source": (None, f.name.split('/')[-1]),
                    "name": (None, name)}
            print(f"PUT DATA: {data}")
            res = requests.put(url, files=data)
            res.raise_for_status()




    def test_1_create_integration(self):
        print(f'\nExecuting {self._testMethodName}')
        url = f'{HTTP_API_ROOT}/config/integrations/{INTEGRATION_NAME}'
        params = {"type": "redis",
                  "db": 0,
                  "host": "127.0.0.1",
                  "port": REDIS_PORT,
                  "stream": "control"}
        try:
            res = requests.put(url, json={"params": params})
            self.assertTrue(res.status_code == 200, res.text)
        except Exception as e:
            self.fail(e)


    def test_2_create_stream(self):
        self.upload_ds(self._testMethodName)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
