import unittest
import csv
import inspect

import pytds

from mindsdb.utilities.config import Config

from common import (
    run_environment,
    get_test_csv,
    TEST_CONFIG,
    MINDSDB_DATABASE
)

TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

EXTERNAL_DS_NAME = 'test_external'

config = Config(TEST_CONFIG)


def query(query, fetch=False, as_dict=True, db='mindsdb_test'):
    integration = config['integrations']['default_mssql']
    conn = pytds.connect(
        user=integration['user'],
        password=integration['password'],
        dsn=integration['host'],
        port=integration['port'],
        as_dict=as_dict,
        database=db,
        autocommit=True
    )

    cur = conn.cursor()
    cur.execute(query)
    res = True
    if fetch:
        res = cur.fetchall()
    cur.close()
    conn.close()

    return res


class MSSQLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment(
            config,
            apis=['mysql'],
            run_docker_db=[],   # NOTE ds should be already runned
            override_integration_config={
                'default_mssql': {
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

        test_csv_path = get_test_csv(TEST_CSV['name'], TEST_CSV['url'])

        res = query("SELECT name FROM master.dbo.sysdatabases where name = 'mindsdb_test'", fetch=True)
        if len(res) == 0:
            query("create database mindsdb_test")
        res = query('''
            select * from sys.schemas where name = 'mindsdb_schema';
        ''', fetch=True)
        if len(res) == 0:
            query('''
                create schema [mindsdb_schema];
            ''')

        # query('create database if not exists test')
        # show tables from test
        test_tables = query(f'''
            select 1 from sysobjects where name='{TEST_DATA_TABLE}' and xtype='U';
        ''', fetch=True)
        if len(test_tables) == 0:
            print('creating test data table...')
            query(f'''
                CREATE TABLE mindsdb_schema.{TEST_DATA_TABLE} (
                    number_of_rooms int,
                    number_of_bathrooms int,
                    sqft int,
                    location varchar(100),
                    days_on_market int,
                    initial_price int,
                    neighborhood varchar(100),
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
                        query(f'''
                            INSERT INTO mindsdb_schema.{TEST_DATA_TABLE} VALUES (
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
                    if i % 100 == 0:
                        print(i)
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
        test_tables = query(f'''
            select 1 from sysobjects where name='{TEST_DATA_TABLE}' and xtype='U';
        ''', fetch=True)
        self.assertTrue(len(test_tables) == 1)

    def test_2_insert_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            exec ('
                insert into predictors (name, predict, select_data_query, training_options)
                values (
                    ''{TEST_PREDICTOR_NAME}'',
                    ''rental_price'',
                    ''select * from mindsdb_test.mindsdb_schema.{TEST_DATA_TABLE} order by sqft offset 0 rows fetch next 100 rows only'',
                    ''{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}''
                )') AT mindsdb;
        """)

        print('predictor record in mindsdb.predictors')
        res = query(f"""
            exec ('SELECT status FROM predictors where name = ''{TEST_PREDICTOR_NAME}''') AT mindsdb;
        """, as_dict=True, fetch=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

    def test_3_externael_ds(self):
        name = f'{TEST_PREDICTOR_NAME}_external'
        models = self.mdb.get_models()
        models = [x['name'] for x in models]
        if name in models:
            self.mdb.delete_model(name)

        query(f"""
            exec ('
                insert into predictors (name, predict, external_datasource, training_options)
                values (
                    ''{name}'',
                    ''rental_price'',
                    ''{EXTERNAL_DS_NAME}'',
                    ''{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}''
                )') AT mindsdb;
        """)

        print('predictor record in mindsdb.predictors')
        res = query(f"""
            exec ('SELECT status FROM predictors where name = ''{name}''') AT mindsdb;
        """, as_dict=True, fetch=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        res = query(f"""
            exec ('
                select
                    rental_price, location, sqft, number_of_rooms,
                    rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
                from
                    mindsdb.{name} where external_datasource=''{EXTERNAL_DS_NAME}''
            ') AT mindsdb;
        """, as_dict=True, fetch=True)

        print('check result')
        self.assertTrue(len(res) > 0)
        self.assertTrue(res[0]['rental_price'] is not None and res[0]['rental_price'] != 'None')
        self.assertTrue(res[0]['location'] is not None and res[0]['location'] != 'None')

    def test_4_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = query(f"""
            exec ('
                select
                    rental_price, location, sqft, number_of_rooms,
                    rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
                from
                    mindsdb.{TEST_PREDICTOR_NAME} where sqft=1000 and location=''good'';
            ') at mindsdb;
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

    def test_5_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        results = query(f"""
            exec ('
                select
                    rental_price, location, sqft, number_of_rooms,
                    rental_price_confidence, rental_price_min, rental_price_max, rental_price_explain
                from
                    mindsdb.{TEST_PREDICTOR_NAME} where select_data_query=''select * from mindsdb_test.mindsdb_schema.{TEST_DATA_TABLE} order by sqft offset 0 rows fetch next 3 rows only '';
            ') at mindsdb;
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

    def test_6_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            exec ('insert into mindsdb.commands (command) values (''delete predictor {TEST_PREDICTOR_NAME}'')') at mindsdb;
        """)

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)

    def test_7_insert_predictor_again(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        self.test_2_insert_predictor()

    def test_8_delete_predictor_by_delete_statement(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            exec ('delete from mindsdb.predictors where name=''{TEST_PREDICTOR_NAME}'' ') at mindsdb;
        """)

        print(f'Predictor {TEST_PREDICTOR_NAME} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(TEST_PREDICTOR_NAME not in models)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
