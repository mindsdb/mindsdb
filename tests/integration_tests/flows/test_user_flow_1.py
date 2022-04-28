import unittest
import requests
from pathlib import Path
import json

from common import (
    HTTP_API_ROOT,
    CONFIG_PATH,
    run_environment
)

from http_test_helpers import (
    wait_predictor_learn,
    check_predictor_exists,
    check_predictor_not_exists,
    check_ds_not_exists,
    check_ds_exists,
    check_ds_analyzable
)

# +++ define test data
TEST_DATASET = 'us_health_insurance'

TO_PREDICT = {
    # 'charges': float,
    'smoker': str
}
CONDITION = {
    'age': 20,
    'sex': 'female'
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'

TEST_INTEGRATION = 'test_integration'
TEST_DS = 'test_ds'
TEST_DS_CSV = 'test_ds_csv'
TEST_PREDICTOR = 'test_predictor'

config = {}


class UserFlowTest_1(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['mysql', 'http']
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

    def test_1_create_integration_via_http(self):
        '''
        check integration is not exists
        create integration
        check new integration values
        '''
        res = requests.get(f'{HTTP_API_ROOT}/config/integrations/{TEST_INTEGRATION}')
        assert res.status_code == 404

        test_integration_data = {}
        test_integration_data.update(config['integrations']['default_mariadb'])
        test_integration_data['publish'] = True
        test_integration_data['database_name'] = TEST_INTEGRATION
        res = requests.put(f'{HTTP_API_ROOT}/config/integrations/{TEST_INTEGRATION}', json={'params': test_integration_data})
        assert res.status_code == 200

        res = requests.get(f'{HTTP_API_ROOT}/config/integrations/{TEST_INTEGRATION}')
        assert res.status_code == 200
        test_integration = res.json()
        assert test_integration['password'] is None
        for key in ['user', 'port', 'host', 'publish']:
            assert test_integration[key] == test_integration_data[key]

    def test_3_create_ds_from_sql_by_http(self):
        '''
        check is no DS with this name
        create DS
        analyse it
        '''
        check_ds_not_exists(TEST_DS)

        data = {
            "integration_id": TEST_INTEGRATION,
            "name": TEST_DS,
            "query": f"select * from test_data.{TEST_DATASET} limit 100;"
        }
        res = requests.put(f'{HTTP_API_ROOT}/datasources/{TEST_DS}', json=data)
        assert res.status_code == 200

        check_ds_exists(TEST_DS)
        check_ds_analyzable(TEST_DS)

    def test_4_create_and_query_predictors(self):
        '''
        check predictor not exists
        learn predictor
        query
        '''
        def test_predictor(predictior_name, datasource_name):
            check_predictor_not_exists(predictior_name)

            data = {
                'to_predict': list(TO_PREDICT.keys()),
                'data_source_name': datasource_name
            }
            res = requests.put(f'{HTTP_API_ROOT}/predictors/{predictior_name}', json=data)
            assert res.status_code == 200

            # wait for https://github.com/mindsdb/mindsdb/issues/1459
            import time
            time.sleep(5)

            check_predictor_exists(predictior_name)

            import time
            time.sleep(10)

            wait_predictor_learn(predictior_name)

            res = requests.post(
                f'{HTTP_API_ROOT}/predictors/{predictior_name}/predict',
                json={'when': CONDITION}
            )
            assert res.status_code == 200
            res = res.json()
            assert len(res) == 1
            res = res[0]
            for field in TO_PREDICT:
                assert field in res
                assert res[field]['predicted_value'] is not None
                assert res[field]['confidence'] > 0

        test_predictor(TEST_PREDICTOR, TEST_DS)

    def test_5_delete(self):
        res = requests.delete(f'{HTTP_API_ROOT}/predictors/{TEST_PREDICTOR}')
        assert res.status_code == 200
        check_predictor_not_exists(TEST_PREDICTOR)

        # for ds_name in [TEST_DS_CSV, TEST_DS]:
        #     res = requests.delete(f'{HTTP_API_ROOT}/datasources/{ds_name}')
        #     assert res.status_code == 200
        #     check_ds_not_exists(ds_name)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
