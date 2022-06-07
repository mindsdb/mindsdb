import unittest
import inspect
from pathlib import Path
import json
import requests

from pymongo import MongoClient

from common import (
    CONFIG_PATH,
    HTTP_API_ROOT,
    run_environment
)

from http_test_helpers import (
    get_predictors_names_list,
    get_integrations_names
)

config = {}

CID_A = 1
CID_B = 2


def get_mongo_predictors(company_id):
    client = MongoClient(host='127.0.0.1', port=int(config['api']['mongodb']['port']))
    client.admin.command({'company_id': company_id, 'need_response': 1})
    return [x['name'] for x in client.mindsdb.predictors.find()]


class CompanyIndependentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['http', 'mongodb']
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

    def test_1_initial_state_http(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        # is no predictors
        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_A)
        self.assertTrue(len(predictors_a) == 0)
        self.assertTrue(len(predictors_b) == 0)

        # is no integrations
        integrations_a = get_integrations_names(company_id=CID_A)
        integrations_b = get_integrations_names(company_id=CID_B)
        self.assertTrue(len(integrations_a) == 0)
        self.assertTrue(len(integrations_b) == 0)

    def test_2_add_integration_http(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        test_integration_data = {}
        test_integration_data.update(config['integrations']['default_postgres'])
        test_integration_data['publish'] = False

        res = requests.put(
            f'{HTTP_API_ROOT}/config/integrations/test_integration_a',
            json={'params': test_integration_data},
            headers={'company-id': f'{CID_A}'}
        )
        self.assertTrue(res.status_code == 200)

        integrations_a = get_integrations_names(company_id=CID_A)
        self.assertTrue(len(integrations_a) == 1 and integrations_a[0] == 'test_integration_a')

        integrations_b = get_integrations_names(company_id=CID_B)
        self.assertTrue(len(integrations_b) == 0)

        res = requests.put(
            f'{HTTP_API_ROOT}/config/integrations/test_integration_b',
            json={'params': test_integration_data},
            headers={'company-id': f'{CID_B}'}
        )
        self.assertTrue(res.status_code == 200)

        integrations_a = get_integrations_names(company_id=CID_A)
        self.assertTrue(len(integrations_a) == 1 and integrations_a[0] == 'test_integration_a')

        integrations_b = get_integrations_names(company_id=CID_B)
        self.assertTrue(len(integrations_b) == 1 and integrations_b[0] == 'test_integration_b')

    def test_4_add_predictors_http(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        params = {
            'integration': 'test_integration_a',
            'query': 'select * from test_data.home_rentals limit 50',
            'to_predict': 'rental_price',
            'kwargs': {
                'time_aim': 5,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/test_p_a'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_A}'})
        self.assertTrue(res.status_code == 200)

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_p_a')
        self.assertTrue(len(predictors_b) == 0)

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_p_a')
        self.assertTrue(len(mongo_predictors_b) == 0)

        params = {
            'integration': 'test_integration_a',
            'query': 'select * from test_data.home_rentals limit 50',
            'to_predict': 'rental_price',
            'kwargs': {
                'time_aim': 5,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/test_p_b'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_B}'})
        # shold not be able to create predictor from foreign ds
        self.assertTrue(res.status_code != 200)

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_p_a')
        self.assertTrue(len(predictors_b) == 0)

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_p_a')
        self.assertTrue(len(mongo_predictors_b) == 0)

        params = {
            'integration': 'test_integration_b',
            'query': 'select * from test_data.home_rentals limit 50',
            'to_predict': 'rental_price',
            'kwargs': {
                'time_aim': 5,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/test_p_b'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_B}'})
        self.assertTrue(res.status_code == 200)

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_p_a')
        self.assertTrue(len(predictors_b) == 1 and predictors_b[0] == 'test_p_b')

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_p_a')
        self.assertTrue(len(mongo_predictors_b) == 1 and mongo_predictors_b[0] == 'test_p_b')

    def test_5_add_predictors_mongo(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        client = MongoClient(host='127.0.0.1', port=int(config['api']['mongodb']['port']))
        client.admin.command({'company_id': CID_A, 'need_response': 1})
        client.mindsdb.predictors.insert_one({
            'name': 'test_mon_p_a',
            'predict': 'rental_price',
            'connection': 'test_integration_a',
            'select_data_query': 'select * from test_data.home_rentals limit 50',
            'training_options': {
                'join_learn_process': True,
                'time_aim': 3
            }
        })

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 2 and mongo_predictors_a[1] == 'test_mon_p_a')
        self.assertTrue(len(mongo_predictors_b) == 1 and mongo_predictors_b[0] == 'test_p_b')

        client = MongoClient(host='127.0.0.1', port=int(config['api']['mongodb']['port']))
        client.admin.command({'company_id': CID_A, 'need_response': 1})
        client.mindsdb.predictors.delete_one({'name': 'test_p_a'})

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_mon_p_a')
        self.assertTrue(len(mongo_predictors_b) == 1 and mongo_predictors_b[0] == 'test_p_b')

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_mon_p_a')
        self.assertTrue(len(predictors_b) == 1 and predictors_b[0] == 'test_p_b')


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
