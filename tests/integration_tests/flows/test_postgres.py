import unittest
import inspect
from pathlib import Path

import pg8000

from mindsdb.utilities.config import Config

from common import (
    MINDSDB_DATABASE,
    run_environment,
    get_test_csv,
    TEST_CONFIG,
    upload_csv,
    DATASETS_COLUMN_TYPES,
    DATASETS_PATH,
    check_prediction_values,
    condition_dict_to_str,
    USE_EXTERNAL_DB_SERVER
)

# +++ define test data
TEST_DATASET = 'used_car_price'

DB_TYPES_MAP = {
    int: 'int',
    float: 'float',
    str: 'text'
}

TO_PREDICT = {
    'engineSize': float,
    'model': str
}
CONDITION = {
    'year': 2017,
    'transmission': 'Manual',
    'mpg': 60.0
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

config = Config(TEST_CONFIG)

to_predict_column_names = list(TO_PREDICT.keys())


def query(query, fetch=False):
    integration = config['integrations']['default_postgres']
    con = pg8000.connect(
        database=integration.get('database', 'postgres'),
        user=integration['user'],
        password=integration['password'],
        host=integration['host'],
        port=integration['port']
    )

    cur = con.cursor()
    res = True
    cur.execute(query)

    if fetch is True:
        rows = cur.fetchall()
        keys = [k[0] if isinstance(k[0], str) else k[0].decode('ascii') for k in cur.description]
        res = [dict(zip(keys, row)) for row in rows]

    con.commit()
    con.close()

    return res


def fetch(q):
    return query(q, fetch=True)


class PostgresTest(unittest.TestCase):
    def get_tables_in(self, schema):
        test_tables = fetch(f"SELECT table_name as name FROM information_schema.tables WHERE table_schema = '{schema}'")
        return [x['name'] for x in test_tables]

    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment(
            config,
            apis=['mysql'],
            override_integration_config={
                'default_postgres': {
                    'enabled': True
                }
            },
            mindsdb_database=MINDSDB_DATABASE
        )
        cls.mdb = mdb

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

        query('create schema if not exists test_data')

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
                    escape='"'
                )

        ds = datastore.get_datasource(EXTERNAL_DS_NAME)
        if ds is not None:
            datastore.delete_datasource(EXTERNAL_DS_NAME)

        short_csv_file_path = get_test_csv(
            f'{EXTERNAL_DS_NAME}.csv',
            test_csv_path,
            lines_count=100,
            rewrite=True,
            column_names=[x[0] for x in DATASETS_COLUMN_TYPES[TEST_DATASET]]
        )
        datastore.save_datasource(EXTERNAL_DS_NAME, 'file', 'test.csv', short_csv_file_path)

    def test_1_initial_state(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        print('Check all testing objects not exists')

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print('Test datasource exists')
        self.assertTrue(TEST_DATA_TABLE in self.get_tables_in('test_data'))

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

        print('predictor record in mindsdb.predictors')
        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR_NAME}'")
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
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

        print('predictor record in mindsdb.predictors')
        res = query(f"select status from {MINDSDB_DATABASE}.predictors where name = '{name}'", fetch=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
        self.assertTrue(name in self.get_tables_in(MINDSDB_DATABASE))

        res = query(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{name}
            where
                external_datasource='{EXTERNAL_DS_NAME}'
        """, fetch=True)

        print('check result')
        self.assertTrue(len(res) > 0)
        for r in res:
            self.assertTrue(check_prediction_values(r, TO_PREDICT))

    def test_4_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = query(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                {condition_dict_to_str(CONDITION)};
        """, fetch=True)

        print('check result')
        self.assertTrue(len(res) == 1)
        self.assertTrue(check_prediction_values(res[0], TO_PREDICT))

    def test_5_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        res = query(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                select_data_query='select * from test_data.{TEST_DATA_TABLE} limit 3'
        """, fetch=True)

        self.assertTrue(len(res) == 3)
        for r in res:
            self.assertTrue(check_prediction_values(r, TO_PREDICT))

    def test_6_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            insert into {MINDSDB_DATABASE}.commands values ('delete predictor {TEST_PREDICTOR_NAME}');
        """)

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print('Test predictor table not exists')
        self.assertTrue(TEST_PREDICTOR_NAME not in self.get_tables_in(MINDSDB_DATABASE))


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
