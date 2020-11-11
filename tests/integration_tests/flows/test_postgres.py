import unittest
import csv
import inspect

import pg8000

from mindsdb.utilities.config import Config

from common import (
    MINDSDB_DATABASE,
    run_environment,
    get_test_csv,
    TEST_CONFIG
)

TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

EXTERNAL_DS_NAME = 'test_external'

config = Config(TEST_CONFIG)


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


class PostgresTest(unittest.TestCase):
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
        test_tables = query("SELECT table_name as name FROM information_schema.tables WHERE table_schema = 'test_data'", fetch=True)
        test_tables = [x['name'] for x in test_tables]

        test_csv_path = get_test_csv(TEST_CSV['name'], TEST_CSV['url'])

        if TEST_DATA_TABLE not in test_tables:
            print('creating test data table...')
            query(f'''
                CREATE TABLE test_data.{TEST_DATA_TABLE} (
                    number_of_rooms int,
                    number_of_bathrooms int,
                    sqft int,
                    location text,
                    days_on_market int,
                    initial_price int,
                    neighborhood text,
                    rental_price int
                )
            ''')

            with open(test_csv_path) as f:
                csvf = csv.reader(f)
                i = 0
                for row in csvf:
                    if i > 0:
                        number_of_rooms = int(row[0])
                        number_of_bathrooms = int(row[1])
                        sqft = int(float(row[2].replace(',', '.')))
                        location = str(row[3])
                        days_on_market = int(row[4])
                        initial_price = int(row[5])
                        neighborhood = str(row[6])
                        rental_price = int(float(row[7]))
                        query(f'''INSERT INTO test_data.{TEST_DATA_TABLE} VALUES (
                            {number_of_rooms},
                            {number_of_bathrooms},
                            {sqft},
                            '{location}',
                            {days_on_market},
                            {initial_price},
                            '{neighborhood}',
                            {rental_price}
                        )''')
                    i += 1
            print('done')

        ds = datastore.get_datasource(EXTERNAL_DS_NAME)
        if ds is not None:
            datastore.delete_datasource(EXTERNAL_DS_NAME)
        short_csv_file_path = get_test_csv(f'{EXTERNAL_DS_NAME}.csv', TEST_CSV['url'], lines_count=300, rewrite=True)
        datastore.save_datasource(EXTERNAL_DS_NAME, 'file', 'test.csv', short_csv_file_path)

    def test_1_initial_state(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        print('Check all testing objects not exists')

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

        print('Test datasource exists')
        test_tables = query("SELECT table_name as name FROM information_schema.tables WHERE table_schema = 'test_data'", fetch=True)
        test_tables = [x['name'] for x in test_tables]
        self.assertTrue(TEST_DATA_TABLE in test_tables)

        print('Test predictor table not exists')
        mindsdb_tables = query(f"SELECT table_name as name FROM information_schema.tables WHERE table_schema = '{MINDSDB_DATABASE}'", fetch=True)
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)

        print('mindsdb.predictors table exists')
        self.assertTrue('predictors' in mindsdb_tables)

        print('mindsdb.commands table exists')
        self.assertTrue('commands' in mindsdb_tables)

    def test_2_insert_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            insert into {MINDSDB_DATABASE}.predictors (name, predict, select_data_query, training_options) values
            (
                '{TEST_PREDICTOR_NAME}',
                'rental_price, location',
                'select * from test_data.{TEST_DATA_TABLE} limit 100',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        print('predictor record in mindsdb.predictors')
        res = query(f"select status from {MINDSDB_DATABASE}.predictors where name = '{TEST_PREDICTOR_NAME}'", fetch=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
        mindsdb_tables = query(f"SELECT table_name as name FROM information_schema.tables WHERE table_schema = '{MINDSDB_DATABASE}'", fetch=True)
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME in mindsdb_tables)

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
                'rental_price, location',
                '{EXTERNAL_DS_NAME}',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        print('predictor record in mindsdb.predictors')
        res = query(f"select status from {MINDSDB_DATABASE}.predictors where name = '{name}'", fetch=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print('predictor table in mindsdb db')
        mindsdb_tables = query(f"SELECT table_name as name FROM information_schema.tables WHERE table_schema = '{MINDSDB_DATABASE}'", fetch=True)
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(name in mindsdb_tables)

        res = query(f"""
            select
                rental_price, location, sqft, number_of_rooms,
                rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
            from
                {MINDSDB_DATABASE}.{name} where external_datasource='{EXTERNAL_DS_NAME}'
        """, fetch=True)

        print('check result')
        self.assertTrue(len(res) > 0)
        self.assertTrue(res[0]['rental_price'] is not None and res[0]['rental_price'] != 'None')
        self.assertTrue(res[0]['location'] is not None and res[0]['location'] != 'None')

    def test_4_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = query(f"""
            select
                rental_price, location, sqft, number_of_rooms,
                rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME} where sqft=1000
        """, fetch=True)

        print('check result')
        self.assertTrue(len(res) == 1)

        res = res[0]

        self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
        self.assertTrue(res['location'] is not None and res['location'] != 'None')
        self.assertTrue(res['sqft'] == 1000)
        self.assertIsInstance(res['rental_price_confidence'], (int, float))
        self.assertIsInstance(res['rental_price_min'], (int, float))
        self.assertIsInstance(res['rental_price_max'], (int, float))
        self.assertIsInstance(res['rental_price_explain'], str)
        self.assertTrue(res['number_of_rooms'] == 'None' or res['number_of_rooms'] is None)

    def test_5_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        results = query(f"""
            select
                rental_price, location, sqft, number_of_rooms,
                rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
            from
                {MINDSDB_DATABASE}.{TEST_PREDICTOR_NAME} where select_data_query='select * from test_data.{TEST_DATA_TABLE} limit 3'
        """, fetch=True)

        print('check result')
        self.assertTrue(len(results) == 3)
        for res in results:
            self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
            self.assertTrue(res['location'] is not None and res['location'] != 'None')
            self.assertIsInstance(res['rental_price_confidence'], (int, float))
            self.assertIsInstance(res['rental_price_min'], (int, float))
            self.assertIsInstance(res['rental_price_max'], (int, float))
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
        mindsdb_tables = query(f"SELECT table_name as name FROM information_schema.tables WHERE table_schema = '{MINDSDB_DATABASE}'", fetch=True)
        mindsdb_tables = [x['name'] for x in mindsdb_tables]
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_tables)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
