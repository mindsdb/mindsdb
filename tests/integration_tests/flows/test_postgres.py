import unittest
import inspect
from pathlib import Path
import json

import psycopg

from common import (
    MINDSDB_DATABASE,
    CONFIG_PATH,
    check_prediction_values,
    condition_dict_to_str,
    run_environment
)

# +++ define test data
TEST_DATASET = 'used_car_price'

TO_PREDICT = {
    'enginesize': str,
    # 'model': str
}
CONDITION = {
    'year': '2017',
    'transmission': 'Manual',
    'mpg': '60.0'
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'

INTEGRATION_NAME = 'default_postgres'

config = {}

to_predict_column_names = list(TO_PREDICT.keys())


def query(query, fetch=False):
    integration = config['integrations'][INTEGRATION_NAME]

    with psycopg.connect(f"host={integration['host']} port={integration['port']} dbname={integration.get('database', 'postgres')} user={integration['user']} password={integration['password']}") as con:
        with con.cursor() as cur:
            res = True
            cur.execute(query)

            if fetch is True:
                rows = cur.fetchall()
                keys = [k[0] if isinstance(k[0], str) else k[0].decode('ascii') for k in cur.description]
                res = [dict(zip(keys, row)) for row in rows]

            con.commit()

    return res


def fetch(q):
    return query(q, fetch=True)


class PostgresTest(unittest.TestCase):
    def get_tables_in(self, schema):
        test_tables = fetch(f"SELECT table_name as name FROM information_schema.tables WHERE table_schema = '{schema}'")
        return [x['name'] for x in test_tables]

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
        res = fetch(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR_NAME}'")
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
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
        self.assertTrue(check_prediction_values(res[0], TO_PREDICT))

    def test_4_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        res = fetch(f"""
            select
                *
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME}
            where
                select_data_query='select * from test_data.{TEST_DATA_TABLE} limit 3'
        """)

        self.assertTrue(len(res) == 3)
        for r in res:
            self.assertTrue(check_prediction_values(r, TO_PREDICT))

    def test_5_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            insert into {MINDSDB_DATABASE}.commands values ('delete predictor {TEST_PREDICTOR_NAME}');
        """)

        self.assertTrue(TEST_PREDICTOR_NAME not in self.get_tables_in(MINDSDB_DATABASE))


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
