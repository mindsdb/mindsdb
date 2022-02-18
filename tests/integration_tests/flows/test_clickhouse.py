import json
import unittest
import requests
import inspect
from pathlib import Path

from common import (
    MINDSDB_DATABASE,
    CONFIG_PATH,
    condition_dict_to_str,
    run_environment
)

# +++ define test data
TEST_DATASET = 'home_rentals'

TO_PREDICT = {
    'rental_price': float
    # 'location': str
}
CONDITION = {
    'sqft': 1000,
    'neighborhood': 'downtown'
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'

INTEGRATION_NAME = 'default_clickhouse'

config = {}

to_predict_column_names = list(TO_PREDICT.keys())


def query(query):
    if 'CREATE ' not in query.upper() and 'INSERT ' not in query.upper():
        query = query.strip('\n ;')
        query += ' FORMAT JSON'

    host = config['integrations'][INTEGRATION_NAME]['host']
    port = config['integrations'][INTEGRATION_NAME]['port']

    connect_string = f'http://{host}:{port}'

    params = {'user': 'default'}
    try:
        params['user'] = config['integrations'][INTEGRATION_NAME]['user']
    except Exception:
        pass

    try:
        params['password'] = config['integrations'][INTEGRATION_NAME]['password']
    except Exception:
        pass

    res = requests.post(
        connect_string,
        data=query,
        params=params
    )

    if res.status_code != 200:
        print(f'ERROR: code={res.status_code} msg={res.text}')
        raise Exception(res.text)

    if ' FORMAT JSON' in query:
        res = res.json()['data']

    return res


def fetch(q):
    return query(q)


class ClickhouseTest(unittest.TestCase):
    def get_tables_in(self, schema):
        test_tables = fetch(f"show tables from {schema}")
        return [x['name'] for x in test_tables]

    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['mysql', 'http'],
            override_config={
                'api': {
                    'mysql': {
                        'ssl': False
                    }
                },
                'integrations': {
                    INTEGRATION_NAME: {
                        'publish': True
                    }
                },
                'permanent_storage': {
                    'location': 'local'
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

        self.assertTrue(TEST_DATA_TABLE in self.get_tables_in('test_data'))

        mindsdb_tables = self.get_tables_in(MINDSDB_DATABASE)
        self.assertTrue(len(mindsdb_tables) >= 2)
        self.assertTrue('predictors' in mindsdb_tables)
        self.assertTrue('commands' in mindsdb_tables)

        data = fetch(f'select * from {MINDSDB_DATABASE}.predictors;')
        self.assertTrue(len(data) == 0)

    def test_2_insert_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            insert into {MINDSDB_DATABASE}.predictors (name, predict, select_data_query, training_options) values
            (
                '{TEST_PREDICTOR_NAME}',
                '{','.join(to_predict_column_names)}',
                'select * from test_data.{TEST_DATA_TABLE} limit 150',
                '{{"join_learn_process": true, "time_aim": 3}}'
            );
        """)

        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR_NAME}'")
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        self.assertTrue(TEST_PREDICTOR_NAME in self.get_tables_in(MINDSDB_DATABASE))

    def test_3_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = fetch(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                {condition_dict_to_str(CONDITION)};
        """)

        print('check result')
        self.assertTrue(len(res) == 1)

        res = res[0]

        self.assertTrue(res['rental_price'] not in (None, 'None'))
        # NOTE in current Clickhouse all int fields returns as strings
        self.assertTrue(res['sqft'] == '1000')
        self.assertIsInstance(res['rental_price_confidence'], float)
        self.assertTrue(isinstance(res['rental_price_min'], (int, float)))
        self.assertTrue(isinstance(res['rental_price_max'], (int, float)))
        self.assertTrue(isinstance(res['rental_price_explain'], (str, dict)))
        self.assertTrue(res['number_of_rooms'] in ('None', None))

    def test_4_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        results = fetch(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                select_data_query='select * from test_data.{TEST_DATA_TABLE} limit 3'
        """)

        print('check result')
        self.assertTrue(len(results) == 3)
        for res in results:
            self.assertTrue(res['rental_price'] not in (None, 'None'))
            self.assertIsInstance(res['rental_price_confidence'], float)
            self.assertTrue(isinstance(res['rental_price_min'], (int, float)))
            self.assertTrue(isinstance(res['rental_price_max'], (int, float)))
            self.assertTrue(isinstance(res['rental_price_explain'], (str, dict)))

    def test_5_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            insert into {MINDSDB_DATABASE}.commands values ('delete predictor {TEST_PREDICTOR_NAME}');
        """)

        print('Test predictor table not exists')
        self.assertTrue(TEST_PREDICTOR_NAME not in self.get_tables_in(MINDSDB_DATABASE))


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
