import unittest
import requests
import asyncio
import time

from mindsdb.utilities.config import Config

from common import (
    MINDSDB_DATABASE,
    HTTP_API_ROOT,
    TEST_CONFIG,
    run_environment,
    stop_mindsdb
)

from http_test_helpers import (
    wait_predictor_learn,
    check_predictor_not_exists,
    check_ds_not_exists,
    check_ds_exists
)

# +++ define test data
TEST_DATASET = 'us_health_insurance'

TO_PREDICT = {
    'smoker': str
}
CONDITION = {
    'age': 20,
    'sex': 'female'
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

TEST_INTEGRATION = 'test_integration'
TEST_DS = 'test_ds'
TEST_PREDICTOR = 'test_predictor'

config = Config(TEST_CONFIG)


class UserFlowTest_1(unittest.TestCase):
    def test_1_wrong_integration(self):
        '''
        start mindsdb with publish integration with wrong password
        try create ds
        change password to correct
        '''
        original_db_password = config['integrations']['default_mariadb']['password']
        self.mdb, datastore = run_environment(
            config,
            apis=['mysql', 'http'],
            override_integration_config={
                'default_mariadb': {
                    'publish': True,
                    'password': 'broken'
                }
            },
            mindsdb_database=MINDSDB_DATABASE
        )

        check_ds_not_exists(TEST_DS)

        # TODO creating DS from unexists integration raise not critical error in code.
        # need fix it and return human-readable error
        # related issue: https://github.com/mindsdb/mindsdb/issues/945
        # data = {
        #     "integration_id": 'unexists_integration',
        #     "name": TEST_DS,
        #     "query": f"select * from test_data.{TEST_DATASET} limit 50;"
        # }
        # res = requests.put(f'{HTTP_API_ROOT}/datasources/{TEST_DS}', json=data)
        # assert res ?

        # check create DS with wrong integration password
        data = {
            "integration_id": 'default_mariadb',
            "name": TEST_DS,
            "query": f"select * from test_data.{TEST_DATASET} limit 100;"
        }
        res = requests.put(f'{HTTP_API_ROOT}/datasources/{TEST_DS}', json=data)
        assert 'Access denied for user' in res.json()['message']

        check_ds_not_exists(TEST_DS)

        # restore password
        res = requests.post(
            f'{HTTP_API_ROOT}/config/integrations/default_mariadb',
            json={'params': {'password': original_db_password}}
        )
        assert res.status_code == 200
        config['integrations']['default_mariadb']['password'] = original_db_password

    def test_2_broke_analisys(self):
        '''
        stop mindsdb while analyse dataset
        '''
        data = {
            "integration_id": 'default_mariadb',
            "name": TEST_DS,
            "query": f"select * from test_data.{TEST_DATASET} limit 100;"
        }
        res = requests.put(f'{HTTP_API_ROOT}/datasources/{TEST_DS}', json=data)
        assert res.status_code == 200

        res = requests.get(f'{HTTP_API_ROOT}/datasources/{TEST_DS}/analyze')
        assert res.status_code == 200

        stop_mindsdb()

        self.mdb, datastore = run_environment(
            config,
            apis=['mysql', 'http'],
            override_integration_config={
                'default_mariadb': {
                    'publish': True
                }
            },
            mindsdb_database=MINDSDB_DATABASE,
            clear_storage=False
        )

        check_ds_exists(TEST_DS)

    def test_3_wrong_predictor(self):
        '''
        try create predictor with wrong parameters,
        close mindsdb while model training
        check mindsdb can start again
        '''
        check_predictor_not_exists(TEST_PREDICTOR)

        data = {
            'to_predict': list(TO_PREDICT.keys()),
            'data_source_name': 'wrong ds'
        }
        res = requests.put(f'{HTTP_API_ROOT}/predictors/{TEST_PREDICTOR}', json=data)
        assert 'Can not find datasource' in res.json()['message']

        check_predictor_not_exists(TEST_PREDICTOR)

        data = {
            'to_predict': list(TO_PREDICT.keys()),
            'data_source_name': TEST_DS
        }
        res = requests.put(f'{HTTP_API_ROOT}/predictors/{TEST_PREDICTOR}', json=data)
        assert res.status_code == 200

        stop_mindsdb()

        self.mdb, datastore = run_environment(
            config,
            apis=['mysql', 'http'],
            override_integration_config={
                'default_mariadb': {
                    'publish': True
                }
            },
            mindsdb_database=MINDSDB_DATABASE,
            clear_storage=False
        )

        # TODO add after this issue will be closed: https://github.com/mindsdb/mindsdb/issues/948
        # check_predictor_not_exists(TEST_PREDICTOR)

        data = {
            'to_predict': list(TO_PREDICT.keys()),
            'data_source_name': TEST_DS
        }
        res = requests.put(f'{HTTP_API_ROOT}/predictors/{TEST_PREDICTOR}_2', json=data)
        assert res.status_code == 200

        wait_predictor_learn(f'{TEST_PREDICTOR}_2')

    def test_4_wrong_prediction(self):
        '''
        close mindsdb while make prediction, then try run it again
        '''
        ioloop = asyncio.get_event_loop()
        if ioloop.is_closed():
            ioloop = asyncio.new_event_loop()
        ioloop.run_in_executor(
            None,
            lambda: requests.post(
                f'{HTTP_API_ROOT}/predictors/{TEST_PREDICTOR}_2/predict',
                json={'when': CONDITION}
            )
        )
        time.sleep(0.5)
        stop_mindsdb()
        ioloop.close()

        self.mdb, datastore = run_environment(
            config,
            apis=['mysql', 'http'],
            override_integration_config={
                'default_mariadb': {
                    'publish': True
                }
            },
            mindsdb_database=MINDSDB_DATABASE,
            clear_storage=False
        )

        res = requests.post(
            f'{HTTP_API_ROOT}/predictors/{TEST_PREDICTOR}_2/predict',
            json={'when': CONDITION}
        )
        assert res.status_code == 200


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
