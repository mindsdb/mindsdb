import os
import unittest
from random import randint
from pathlib import Path
from uuid import uuid1
import json

import requests

from common import (
    CONFIG_PATH,
    run_environment
)

rand = randint(0, pow(10, 12))
ds_name = f'hr_ds_{rand}'
pred_name = f'hr_predictor_{rand}'
root = 'http://localhost:47334/api'


class HTTPTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['http'],
            override_config={
                'integrations': {
                    'default_mariadb': {
                        'publish': True
                    },
                    'default_clickhouse': {
                        'publish': True
                    }
                }
            }
        )

        cls.config = json.loads(
            Path(CONFIG_PATH).read_text()
        )

        cls.initial_integrations_names = list(cls.config['integrations'].keys())

    def test_1_config(self):
        print('Hi, I\'m just here to make sure mindsdb boot and shutdown work')


if __name__ == '__main__':
    unittest.main(failfast=True)
