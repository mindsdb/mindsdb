import unittest
import inspect
from pathlib import Path
import json

import mysql.connector

from mindsdb.interfaces.state.config import Config

from common import (
    USE_EXTERNAL_DB_SERVER,
    DATASETS_COLUMN_TYPES,
    MINDSDB_DATABASE,
    DATASETS_PATH,
    TEST_CONFIG,
    check_prediction_values,
    get_all_pridict_fields,
    run_environment,
    make_test_csv,
    upload_csv
)

# +++ define test data
TEST_DATASET = 'hdi'

DB_TYPES_MAP = {
    int: 'int',
    float: 'float',
    str: 'varchar(255)'
}

TO_PREDICT = {
    'GDP_per_capita_USD': int,
    'Development_Index': str
}
CONDITION = {
    'Population': 2044147,
    'Pop_Density': 13.9
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

config = Config(TEST_CONFIG)

to_predict_column_names = list(TO_PREDICT.keys())


def query(q, as_dict=False, fetch=False):
    con = mysql.connector.connect(
        host=config['integrations']['default_mysql']['host'],
        port=config['integrations']['default_mysql']['port'],
        user=config['integrations']['default_mysql']['user'],
        passwd=config['integrations']['default_mysql']['password'],
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


class MySQLDBTest(unittest.TestCase):
    def get_tables_in(self, schema):
        test_tables = fetch(f'show tables from {schema}', as_dict=False)
        return [x[0] for x in test_tables]

    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment(
            config,
            apis=['mysql'],
            override_integration_config={
                'default_mysql': {
                    'publish': True
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
                    csv_path=test_csv_path
                )

        ds = datastore.get_datasource(EXTERNAL_DS_NAME)
        if ds is not None:
            datastore.delete_datasource(EXTERNAL_DS_NAME)

        data = fetch(f'select * from test_data.{TEST_DATA_TABLE} limit 50', as_dict=True)
        external_datasource_csv = make_test_csv(EXTERNAL_DS_NAME, data)
        datastore.save_datasource(EXTERNAL_DS_NAME, 'file', 'test.csv', external_datasource_csv)

    def test_1_initial_state(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        print('Check all testing objects not exists')

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print('Test mindsdb tables exists')
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
                'select * from test_data.{TEST_DATA_TABLE} limit 50',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        print('predictor record in mindsdb.predictors')
        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR_NAME}'", as_dict=True)
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
        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{name}'", as_dict=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
        self.assertTrue(name in self.get_tables_in(MINDSDB_DATABASE))

        fields = get_all_pridict_fields(TO_PREDICT)
        res = fetch(f"""
            select
                {','.join(fields)}
            from
                {MINDSDB_DATABASE}.{name}
            where
                external_datasource='{EXTERNAL_DS_NAME}'
        """, as_dict=True)

        print('check result')
        self.assertTrue(len(res) > 0)
        for r in res:
            self.assertTrue(check_prediction_values(r, TO_PREDICT))

    def test_4_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        fields = get_all_pridict_fields(TO_PREDICT)
        conditions = json.dumps(CONDITION)
        res = fetch(f"""
            select
                {','.join(fields)}
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                when_data='{conditions}';
        """, as_dict=True)

        print('check result')
        self.assertTrue(len(res) == 1)
        self.assertTrue(check_prediction_values(res[0], TO_PREDICT))

    def test_5_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        res = fetch(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                select_data_query='select * from test_data.{TEST_DATA_TABLE} limit 3';
        """, as_dict=True)

        print('check result')
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

    def test_7_insert_predictor_again(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        self.test_2_insert_predictor()

    def test_8_delete_predictor_by_delete_statement(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            delete from {MINDSDB_DATABASE}.predictors where name='{TEST_PREDICTOR_NAME}';
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
