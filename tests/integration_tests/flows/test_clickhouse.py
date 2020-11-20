import unittest
import requests
import inspect
from pathlib import Path

from mindsdb.utilities.config import Config

from common import (
    USE_EXTERNAL_DB_SERVER,
    DATASETS_COLUMN_TYPES,
    MINDSDB_DATABASE,
    DATASETS_PATH,
    TEST_CONFIG,
    condition_dict_to_str,
    run_environment,
    make_test_csv,
    upload_csv
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

config = Config(TEST_CONFIG)

to_predict_column_names = list(TO_PREDICT.keys())


def query(query):
    if 'CREATE ' not in query.upper() and 'INSERT ' not in query.upper():
        query = query.strip('\n ;')
        query += ' FORMAT JSON'

    host = config['integrations']['default_clickhouse']['host']
    port = config['integrations']['default_clickhouse']['port']

    connect_string = f'http://{host}:{port}'

    params = {'user': 'default'}
    try:
        params['user'] = config['integrations']['default_clickhouse']['user']
    except Exception:
        pass

    try:
        params['password'] = config['integrations']['default_clickhouse']['password']
    except Exception:
        pass

    res = requests.post(
        connect_string,
        data=query,
        params=params
    )

    if res.status_code != 200:
        print(f'ERROR: code={res.status_code} msg={res.text}')
        raise Exception()

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
        mdb, datastore = run_environment(
            config,
            apis=['mysql'],
            override_integration_config={
                'default_clickhouse': {
                    'enabled': True
                }
            },
            override_api_config={
                'mysql': {
                    'ssl': False
                }
            },
            mindsdb_database=MINDSDB_DATABASE
        )
        cls.mdb = mdb

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

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
                    csv_path=test_csv_path,
                    template='create table test_data.%s (%s) ENGINE = MergeTree() ORDER BY days_on_market PARTITION BY location'
                )

        ds = datastore.get_datasource(EXTERNAL_DS_NAME)
        if ds is not None:
            datastore.delete_datasource(EXTERNAL_DS_NAME)

        data = fetch(f'select * from test_data.{TEST_DATA_TABLE} limit 50')
        external_datasource_csv = make_test_csv(EXTERNAL_DS_NAME, data)
        datastore.save_datasource(EXTERNAL_DS_NAME, 'file', 'test.csv', external_datasource_csv)

    def test_1_initial_state(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        print('Check all testing objects not exists')

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        self.assertTrue(TEST_DATA_TABLE in self.get_tables_in('test_data'))

        print('Test predictor table not exists')
        mindsdb_tables = self.get_tables_in(MINDSDB_DATABASE)
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)
        self.assertTrue('predictors' in mindsdb_tables)
        self.assertTrue('commands' in mindsdb_tables)

    def test_2_insert_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            insert into {MINDSDB_DATABASE}.predictors (name, predict, select_data_query, training_options) values
            (
                '{TEST_PREDICTOR_NAME}',
                '{','.join(to_predict_column_names)}',
                'select * from test_data.{TEST_DATA_TABLE} limit 100',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR_NAME}'")
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        self.assertTrue(TEST_PREDICTOR_NAME in self.get_tables_in(MINDSDB_DATABASE))

    def test_3_externael_ds(self):
        name = f'{TEST_PREDICTOR_NAME}_external'
        models = self.mdb.get_models()
        models = [x['name'] for x in models]
        if name in models:
            self.mdb.delete_model(name)

        query(f"""
            insert into {MINDSDB_DATABASE}.predictors (name, predict, external_datasource, training_options) values
            (
                '{name}',
                '{','.join(to_predict_column_names)}',
                '{EXTERNAL_DS_NAME}',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{name}'")
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        self.assertTrue(name in self.get_tables_in(MINDSDB_DATABASE))

        res = fetch(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{name}
            where
                external_datasource='{EXTERNAL_DS_NAME}'
        """)

        print('check result')
        self.assertTrue(len(res) > 0)
        self.assertTrue(res[0]['rental_price'] is not None and res[0]['rental_price'] != 'None')
        self.assertTrue(res[0]['location'] is not None and res[0]['location'] != 'None')

    def test_4_query_predictor(self):
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

        self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
        self.assertTrue(res['location'] is not None and res['location'] != 'None')
        # NOTE in current Clickhouse all int fields returns as strings
        self.assertTrue(res['sqft'] == '1000')
        self.assertIsInstance(res['rental_price_confidence'], float)
        self.assertIsInstance(res['rental_price_min'], int)
        self.assertIsInstance(res['rental_price_max'], int)
        self.assertIsInstance(res['rental_price_explain'], str)
        self.assertTrue(res['number_of_rooms'] == 'None' or res['number_of_rooms'] is None)

    def test_5_range_query(self):
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
            self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
            self.assertTrue(res['location'] is not None and res['location'] != 'None')
            self.assertIsInstance(res['rental_price_confidence'], float)
            self.assertIsInstance(res['rental_price_min'], int)
            self.assertIsInstance(res['rental_price_max'], int)
            self.assertIsInstance(res['rental_price_explain'], str)

    def test_6_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            insert into {MINDSDB_DATABASE}.commands values ('delete predictor {TEST_PREDICTOR_NAME}');
        """)

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print('Test predictor table not exists')
        mindsdb_tables = query(f'show tables from {MINDSDB_DATABASE}')
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
