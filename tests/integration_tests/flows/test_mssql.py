import unittest
import inspect
from pathlib import Path
import json
import requests

import pytds

from common import (
    MINDSDB_DATABASE,
    condition_dict_to_str,
    run_environment,
    CONFIG_PATH,
    HTTP_API_ROOT
)

# +++ define test data
TEST_DATASET = 'home_rentals'

DB_TYPES_MAP = {
    int: 'Int32',
    float: 'Float32',
    str: 'String'
}

TO_PREDICT = {
    'rental_price': float,
    'location': str
}
CONDITION = {
    'sqft': 1000,
    'neighborhood': 'downtown'
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

INTEGRATION_NAME = 'default_mssql'

config = {}

to_predict_column_names = list(TO_PREDICT.keys())


def query(query, fetch=False, as_dict=True, db='mindsdb_test'):
    integration = config['integrations'][INTEGRATION_NAME]
    conn = pytds.connect(
        user=integration['user'],
        password=integration['password'],
        database=integration.get('database', 'master'),
        dsn=integration['host'],
        port=integration['port'],
        as_dict=as_dict,
        autocommit=True  # .commit() doesn't work
    )

    cur = conn.cursor()
    cur.execute(query)
    res = True
    if fetch:
        res = cur.fetchall()
    cur.close()
    conn.close()

    return res


def fetch(q, as_dict=True):
    return query(q, as_dict=as_dict, fetch=True)


class MSSQLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['mysql', 'http'],
            override_config={
                'integrations': {
                    INTEGRATION_NAME: {
                        'publish': True
                    }
                }
            }
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

    def test_1_initial_state(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        print('Check all testing objects not exists')

        # 'predictors' exists and empty
        predictors = fetch(f'''
            exec ('
                select * from mindsdb.predictors
            ') AT {MINDSDB_DATABASE};
        ''')

        self.assertTrue(len(predictors) == 0)

    def test_2_put_external_ds(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        params = {
            'name': EXTERNAL_DS_NAME,
            'query': f'select * from test_data.{TEST_DATA_TABLE} order by sqft offset 0 rows fetch next 50 rows only',
            'integration_id': INTEGRATION_NAME
        }

        url = f'{HTTP_API_ROOT}/datasources/{EXTERNAL_DS_NAME}'
        res = requests.put(url, json=params)
        self.assertTrue(res.status_code == 200)
        ds_data = res.json()

        self.assertTrue(ds_data['source_type'] == INTEGRATION_NAME)
        self.assertTrue(ds_data['row_count'] == 50)

        url = f'{HTTP_API_ROOT}/datasources'
        res = requests.get(url)
        self.assertTrue(res.status_code == 200)
        ds_data = res.json()
        self.assertTrue(len(ds_data) == 1)

    def test_3_insert_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            exec ('
                insert into predictors (name, predict, select_data_query, training_options)
                values (
                    ''{TEST_PREDICTOR_NAME}'',
                    ''{','.join(to_predict_column_names)}'',
                    ''select * from test_data.{TEST_DATA_TABLE} order by sqft offset 0 rows fetch next 100 rows only'',
                    ''{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}''
                )') AT {MINDSDB_DATABASE};
        """)

        print('predictor record in mindsdb.predictors')
        res = query(f"""
            exec ('SELECT status FROM predictors where name = ''{TEST_PREDICTOR_NAME}''') AT {MINDSDB_DATABASE};
        """, as_dict=True, fetch=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

    def test_4_externael_ds(self):
        name = f'{TEST_PREDICTOR_NAME}_external'

        query(f"""
            exec ('
                insert into predictors (name, predict, external_datasource, training_options)
                values (
                    ''{name}'',
                    ''rental_price'',
                    ''{EXTERNAL_DS_NAME}'',
                    ''{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}''
                )') AT {MINDSDB_DATABASE};
        """)

        print('predictor record in mindsdb.predictors')
        res = query(f"""
            exec ('SELECT status FROM predictors where name = ''{name}''') AT {MINDSDB_DATABASE};
        """, as_dict=True, fetch=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        res = query(f"""
            exec ('
                select
                    *
                from
                    mindsdb.{name}
                where
                    external_datasource=''{EXTERNAL_DS_NAME}''
            ') AT {MINDSDB_DATABASE};
        """, as_dict=True, fetch=True)

        print('check result')
        self.assertTrue(len(res) > 0)
        self.assertTrue(res[0]['rental_price'] is not None and res[0]['rental_price'] != 'None')
        self.assertTrue(res[0]['location'] is not None and res[0]['location'] != 'None')

    def test_5_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = query(f"""
            exec ('
                select
                    *
                from
                    {TEST_PREDICTOR_NAME}
                where
                    {condition_dict_to_str(CONDITION).replace("'", "''")};
            ') at {MINDSDB_DATABASE};
        """, as_dict=True, fetch=True)

        print('check result')
        self.assertTrue(len(res) == 1)

        res = res[0]

        self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
        self.assertTrue(res['location'] is not None and res['location'] != 'None')
        self.assertTrue(res['sqft'] == '1000')
        self.assertIsInstance(res['rental_price_confidence'], str)
        self.assertIsInstance(res['rental_price_min'], str)
        self.assertIsInstance(res['rental_price_max'], str)
        self.assertIsInstance(res['rental_price_explain'], str)
        self.assertTrue(res['number_of_rooms'] == 'None' or res['number_of_rooms'] is None)

    def test_6_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        results = query(f"""
            exec ('
                select
                    *
                from
                    {TEST_PREDICTOR_NAME}
                where
                    select_data_query=''select * from test_data.{TEST_DATA_TABLE} order by sqft offset 0 rows fetch next 3 rows only '';
            ') at {MINDSDB_DATABASE};
        """, as_dict=True, fetch=True)

        print('check result')
        self.assertTrue(len(results) == 3)
        for res in results:
            self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
            self.assertTrue(res['location'] is not None and res['location'] != 'None')
            self.assertIsInstance(res['rental_price_confidence'], str)
            self.assertIsInstance(res['rental_price_min'], str)
            self.assertIsInstance(res['rental_price_max'], str)
            self.assertIsInstance(res['rental_price_explain'], str)

    def test_7_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            exec ('insert into mindsdb.commands (command) values (''delete predictor {TEST_PREDICTOR_NAME}'')') at {MINDSDB_DATABASE};
        """)

        predictors = fetch(f'''
            exec ('
                select * from mindsdb.predictors
            ') AT {MINDSDB_DATABASE};
        ''')
        predictors = [x['name'] for x in predictors]
        self.assertTrue(TEST_PREDICTOR_NAME not in predictors)

    def test_8_delete_predictor_by_delete_statement(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        name = f'{TEST_PREDICTOR_NAME}_external'

        query(f"""
            exec ('delete from predictors where name=''{name}'' ') at {MINDSDB_DATABASE};
        """)

        predictors = fetch(f'''
            exec ('
                select * from mindsdb.predictors
            ') AT {MINDSDB_DATABASE};
        ''')
        predictors = [x['name'] for x in predictors]
        self.assertTrue(name not in predictors)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
