import unittest
import inspect
from pathlib import Path
import json

import mysql.connector

from common import (
    USE_EXTERNAL_DB_SERVER,
    MINDSDB_DATABASE,
    CONFIG_PATH,
    check_prediction_values,
    get_all_pridict_fields,
    condition_dict_to_str,
    run_environment
)

# +++ define test data
TEST_DATASET = 'us_health_insurance'

TO_PREDICT = {
    'charges': float
    # 'smoker': str
}
CONDITION = {
    'age': 20,
    'sex': 'female'
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'

INTEGRATION_NAME = 'default_mariadb'

config = {}

to_predict_column_names = list(TO_PREDICT.keys())


def query(q, as_dict=False, fetch=False):
    con = mysql.connector.connect(
        host=config['integrations'][INTEGRATION_NAME]['host'],
        port=config['integrations'][INTEGRATION_NAME]['port'],
        user=config['integrations'][INTEGRATION_NAME]['user'],
        passwd=config['integrations'][INTEGRATION_NAME]['password'],
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


class MariaDBTest(unittest.TestCase):
    def get_tables_in(self, schema):
        test_tables = fetch(f'show tables from {schema}', as_dict=False)
        return [x[0] for x in test_tables]

    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['mysql', 'http'],
            override_config={
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
                'select * from test_data.{TEST_DATA_TABLE} limit 100',
                '{{"join_learn_process": true, "time_aim": 3}}'
            );
        """)

        print('predictor record in mindsdb.predictors')
        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR_NAME}'", as_dict=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
        self.assertTrue(TEST_PREDICTOR_NAME in self.get_tables_in(MINDSDB_DATABASE))

    def test_3_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        fields = get_all_pridict_fields(TO_PREDICT)
        res = fetch(f"""
            select
                {','.join(fields)}
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                {condition_dict_to_str(CONDITION)};
        """)

        self.assertTrue(len(res) == 1)
        self.assertTrue(check_prediction_values(res[0], TO_PREDICT))

    def test_4_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        fields = get_all_pridict_fields(TO_PREDICT)
        res = fetch(f"""
            select
                {','.join(fields)}
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                select_data_query='select * from test_data.{TEST_DATA_TABLE} limit 3';
        """)

        self.assertTrue(len(res) == 3)
        for r in res:
            self.assertTrue(check_prediction_values(r, TO_PREDICT))

    def test_5_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            insert into {MINDSDB_DATABASE}.commands values ('delete predictor {TEST_PREDICTOR_NAME}');
        """)

        print('Test predictor table not exists')
        self.assertTrue(TEST_PREDICTOR_NAME not in self.get_tables_in(MINDSDB_DATABASE))

    # def test_6_delete_predictor_by_delete_statement(self):
    #     print(f'\nExecuting {inspect.stack()[0].function}')
    #     # NOTE looks like error in mariadb CONNECT engine: we get MINDSDB_DATABASE in DELETE query,
    #     # instead of database in engine config.
    #     if USE_EXTERNAL_DB_SERVER:
    #         return

    #     name = f'{TEST_PREDICTOR_NAME}_external'

    #     query(f"""
    #         delete from mindsdb.predictors where name='{name}';
    #     """)

    #     print('Test predictor table not exists')
    #     self.assertTrue(name not in self.get_tables_in(MINDSDB_DATABASE))


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
