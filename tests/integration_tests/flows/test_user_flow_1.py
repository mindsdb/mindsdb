import unittest
import requests
from pathlib import Path
import time

import mysql.connector

from mindsdb.utilities.config import Config

from common import (
    USE_EXTERNAL_DB_SERVER,
    DATASETS_COLUMN_TYPES,
    MINDSDB_DATABASE,
    DATASETS_PATH,
    TEST_CONFIG,
    run_environment,
    make_test_csv,
    upload_csv
)

from http_test_helpers import (
    wait_predictor_learn,
    check_predictor_exists,
    check_predictor_not_exists,
    check_ds_not_exists,
    check_ds_exists,
    check_ds_analyzable
)

api_root = 'http://localhost:47334/api'

# +++ define test data
TEST_DATASET = 'us_health_insurance'

DB_TYPES_MAP = {
    int: 'int',
    float: 'float',
    str: 'varchar(255)'
}

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
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

TEST_INTEGRATION = 'test_integration'
TEST_DS = 'test_ds'
TEST_DS_CSV = 'test_ds_csv'
TEST_PREDICTOR = 'test_predictor'
TEST_PREDICTOR_CSV = 'test_predictor_csv'

config = Config(TEST_CONFIG)

to_predict_column_names = list(TO_PREDICT.keys())


def query(q, as_dict=False, fetch=False):
    con = mysql.connector.connect(
        host=config['integrations']['default_mariadb']['host'],
        port=config['integrations']['default_mariadb']['port'],
        user=config['integrations']['default_mariadb']['user'],
        passwd=config['integrations']['default_mariadb']['password'],
        db=MINDSDB_DATABASE
    )

    cur = con.cursor(dictionary=as_dict)
    cur.execute(q)
    res = True
    if fetch:
        res = cur.fetchall()
    con.commit()
    con.close()
    return res


def fetch(q, as_dict=True):
    return query(q, as_dict, fetch=True)


class UserFlowTest_1(unittest.TestCase):
    def get_tables_in(self, schema):
        test_tables = fetch(f'show tables from {schema}', as_dict=False)
        return [x[0] for x in test_tables]

    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment(
            config,
            apis=['mysql', 'http'],
            mindsdb_database=MINDSDB_DATABASE
        )
        cls.mdb = mdb

        query('create database if not exists test_data')

        if not USE_EXTERNAL_DB_SERVER:
            test_csv_path = Path(DATASETS_PATH).joinpath(TEST_DATASET).joinpath('data.csv')
            if TEST_DATA_TABLE not in cls.get_tables_in(cls, 'test_data'):
                print('creating test data table...')
                upload_csv(
                    query=query,
                    columns_map=DATASETS_COLUMN_TYPES[TEST_DATASET],
                    db_types_map=DB_TYPES_MAP,
                    table_name=TEST_DATA_TABLE,
                    csv_path=test_csv_path
                )


        data = fetch(f'select * from test_data.{TEST_DATA_TABLE} limit 50', as_dict=True)
        cls.external_datasource_csv_path = make_test_csv(EXTERNAL_DS_NAME, data)

    def test_1_create_integration_via_http(self):
        '''
        check integration is not exists
        create integration
        check new integration values
        '''
        res = requests.get(f'{api_root}/config/integrations/{TEST_INTEGRATION}')
        assert res.status_code == 404

        test_integration_data = {}
        test_integration_data.update(config['integrations']['default_mariadb'])
        test_integration_data['enabled'] = True
        test_integration_data['database_name'] = TEST_INTEGRATION
        res = requests.put(f'{api_root}/config/integrations/{TEST_INTEGRATION}', json={'params': test_integration_data})
        assert res.status_code == 200

        res = requests.get(f'{api_root}/config/integrations/{TEST_INTEGRATION}')
        assert res.status_code == 200
        test_integration = res.json()
        assert test_integration['password'] is None
        for key in ['user', 'port', 'host', 'enabled']:
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
        res = requests.put(f'{api_root}/datasources/{TEST_DS}', json=data)
        assert res.status_code == 200

        check_ds_exists(TEST_DS)
        check_ds_analyzable(TEST_DS)

    def test_4_create_ds_from_csv_by_http(self):
        '''
        same for csv-ds
        '''
        check_ds_not_exists(TEST_DS_CSV)

        with open(self.external_datasource_csv_path, 'rb') as f:
            d = f.read()
        res = requests.put(
            f'{api_root}/datasources/{TEST_DS_CSV}',
            files={
                'file': ('data.csv', d, 'text/csv'),
                'name': (None, TEST_DS_CSV),
                'source_type': (None, 'file'),
                'source': (None, 'data.csv')
            }
        )
        assert res.status_code == 200

        check_ds_exists(TEST_DS_CSV)
        check_ds_analyzable(TEST_DS_CSV)

    def test_5_create_and_query_predictors(self):
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
            res = requests.put(f'{api_root}/predictors/{predictior_name}', json=data)
            assert res.status_code == 200

            time.sleep(5)

            check_predictor_exists(predictior_name)

            wait_predictor_learn(predictior_name)

            res = requests.post(
                f'{api_root}/predictors/{predictior_name}/predict',
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
        test_predictor(TEST_PREDICTOR_CSV, TEST_DS_CSV)

    def test_6_delete(self):
        for predictor_name in [TEST_PREDICTOR, TEST_PREDICTOR_CSV]:
            res = requests.delete(f'{api_root}/predictors/{predictor_name}')
            assert res.status_code == 200
            check_predictor_not_exists(predictor_name)

        for ds_name in [TEST_DS_CSV, TEST_DS]:
            res = requests.delete(f'{api_root}/datasources/{ds_name}')
            assert res.status_code == 200
            check_ds_not_exists(ds_name)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
