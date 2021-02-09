import unittest
import requests
import json
from pathlib import Path

import mysql.connector

from common import (
    MINDSDB_DATABASE,
    HTTP_API_ROOT,
    run_environment,
    make_test_csv,
    CONFIG_PATH,
    stop_mindsdb,
    condition_dict_to_str,
    check_prediction_values
)

from http_test_helpers import (
    check_ds_not_exists,
    check_ds_exists,
    check_ds_analyzable
)


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
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

TEST_INTEGRATION = 'test_integration'
TEST_DS = 'test_ds'
TEST_DS_CSV = 'test_ds_csv'
TEST_PREDICTOR = 'test_predictor_name_conflict_fix'
TEST_PREDICTOR_CSV = 'test_predictor_csv'

config = {}

to_predict_column_names = list(TO_PREDICT.keys())


def query(q, as_dict=False, fetch=False):
    con = mysql.connector.connect(
        host=config['integrations']['default_mariadb']['host'],
        port=config['integrations']['default_mariadb']['port'],
        user=config['integrations']['default_mariadb']['user'],
        passwd=config['integrations']['default_mariadb']['password']
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


class UserFlowTest_2(unittest.TestCase):
    def get_tables_in(self, schema):
        test_tables = fetch(f'show tables from {schema}', as_dict=False)
        return [x[0] for x in test_tables]

    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['http']
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

        data = fetch(f'select * from test_data.{TEST_DATA_TABLE} limit 50', as_dict=True)
        cls.external_datasource_csv_path = make_test_csv(EXTERNAL_DS_NAME, data)

    def test_1_upload_ds(self):
        check_ds_not_exists(TEST_DS_CSV)

        with open(self.external_datasource_csv_path, 'rb') as f:
            d = f.read()
        res = requests.put(
            f'{HTTP_API_ROOT}/datasources/{TEST_DS_CSV}',
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

    def test_2_add_integration(self):
        test_integration_data = {}
        test_integration_data.update(config['integrations']['default_mariadb'])
        test_integration_data['publish'] = True
        test_integration_data['database_name'] = TEST_INTEGRATION
        res = requests.put(f'{HTTP_API_ROOT}/config/integrations/{TEST_INTEGRATION}', json={'params': test_integration_data})
        assert res.status_code == 200

    def test_3_restart_and_connect(self):
        stop_mindsdb()

        run_environment(
            apis=['mysql'],
            override_config={
                'integrations': {
                    'default_mariadb': {
                        'publish': False
                    }
                }
            }
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

    def test_4_learn_predictor(self):
        query(f"""
            insert into {MINDSDB_DATABASE}.predictors (name, predict, external_datasource, training_options) values
            (
                '{TEST_PREDICTOR}',
                '{','.join(to_predict_column_names)}',
                '{TEST_DS_CSV}',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        print('predictor record in mindsdb.predictors')
        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR}'")
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
        self.assertTrue(TEST_PREDICTOR in self.get_tables_in(MINDSDB_DATABASE))

    def test_5_make_query(self):
        res = fetch(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR}
            where
                {condition_dict_to_str(CONDITION)};
        """)

        self.assertTrue(len(res) == 1)
        self.assertTrue(check_prediction_values(res[0], TO_PREDICT))


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
