import unittest
import requests
import os
import csv
import time
import inspect
import subprocess
import pathlib
import psutil
import atexit

import mysql.connector

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.utilities.config import Config
from mindsdb.interfaces.mariadb.mariadb import Mariadb

TEST_CONFIG = '/path_to/config.json'

test_csv = 'tests/temp/home_rentals.csv'
test_data_table = 'home_rentals'
test_predictor_name = 'test_predictor'


config = Config(TEST_CONFIG)

def query(q, as_dict=False):
    con = mysql.connector.connect(
        host=config['integrations']['default_mariadb']['host'],
        port=config['integrations']['default_mariadb']['port'],
        user=config['integrations']['default_mariadb']['user'],
        passwd=config['integrations']['default_mariadb']['password'],
        db='mindsdb'
    )

    cur = con.cursor(dictionary=as_dict)
    cur.execute(q)
    res = True
    try:
        res = cur.fetchall()
    except:
        pass
    con.commit()
    con.close()
    return res

class MariaDBTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        for mode in ['train','test']:
            os.system(f'rm {test_csv}')
            cls.mdb = MindsdbNative(config)

            if os.path.isfile(test_csv) is False:
                r = requests.get(f"https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/churn/dataset/{mode}.csv")
                with open(test_csv, 'wb') as f:
                    f.write(r.content)

            models = cls.mdb.get_models()
            models = [x['name'] for x in models]
            if test_predictor_name in models:
                cls.mdb.delete_model(test_predictor_name)

            query('create database if not exists test')
            test_tables = query('show tables from test')
            test_tables = [x[0] for x in test_tables]
            if test_data_table not in test_tables:
                query(f'DROP TABLE IF EXISTS data.{test_data_table}_{mode}')
                query(f'''
                    CREATE TABLE data.{test_data_table}_{mode} (
                        CreditScore int,
                        Geography varchar(300),
                        Gender varchar(300),
                        Age int,
                        Tenure int,
                        Balance float,
                        NumOfProducts int,
                        HasCrCard int,
                        IsActiveMember int,
                        EstimatedSalary float,
                        Exited int
                    )
                ''')
                with open(test_csv) as f:
                    csvf = csv.reader(f)
                    i = 0
                    for row in csvf:
                        if i > 0:
                            CreditScore = int(row[0])
                            Geography = str(row[1])
                            Gender = str(row[2])
                            Age = int(row[3])
                            Tenure = int(row[4])
                            Balance = float(row[5])
                            NumOfProducts = int(row[6])
                            HasCrCard = int(row[7])
                            IsActiveMember = int(row[8])
                            EstimatedSalary = float(row[9])
                            Exited = int(row[10])

                            query(f'''INSERT INTO data.{test_data_table}_{mode} VALUES (
                                {CreditScore},
                                '{Geography}',
                                '{Gender}',
                                {Age},
                                {Tenure},
                                {Balance},
                                {NumOfProducts},
                                {HasCrCard},
                                {IsActiveMember},
                                {EstimatedSalary},
                                {Exited}
                            )''')
                        i += 1
        os.system(f'rm {test_csv}')

    def test_1_initial_state(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        print('Check all testing objects not exists')
        
        print(f'Predictor {test_predictor_name} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(test_predictor_name not in models)

        print(f'Test datasource exists')
        test_tables = query('show tables from test')
        test_tables = [x[0] for x in test_tables]
        self.assertTrue(test_data_table in test_tables)

        print(f'Test predictor table not exists')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x[0] for x in mindsdb_tables]
        self.assertTrue(test_predictor_name not in mindsdb_tables)

        print(f'mindsdb.predictors table exists')
        self.assertTrue('predictors' in mindsdb_tables)

        print(f'mindsdb.commands table exists')
        self.assertTrue('commands' in mindsdb_tables)


    def test_2_insert_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            insert into mindsdb.predictors (name, predict_cols, select_data_query, training_options) values
            (
                '{test_predictor_name}',
                'rental_price, location',
                'select * from test.{test_data_table} limit 100',
                '{{"join_learn_process": true, "stop_training_in_x_seconds": 3}}'
            );
        """)

        print(f'predictor record in mindsdb.predictors')
        res = query(f"select status from mindsdb.predictors where name = '{test_predictor_name}'", as_dict=True)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['status'] == 'complete')

        print(f'predictor table in mindsdb db')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x[0] for x in mindsdb_tables]
        self.assertTrue(test_predictor_name in mindsdb_tables)

    def test_3_query_predictor(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        res = query(f"""
            select
                rental_price, location, sqft, $rental_price_confidence, number_of_rooms
            from
                mindsdb.{test_predictor_name} where sqft=1000;
        """, as_dict=True)

        print('check result')
        self.assertTrue(len(res) == 1)

        res = res[0]

        self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
        self.assertTrue(res['location'] is not None and res['location'] != 'None')
        self.assertTrue(res['sqft'] == 1000)
        self.assertIsInstance(res['$rental_price_confidence'], float)
        self.assertTrue(res['number_of_rooms'] == 'None' or res['number_of_rooms'] is None)

    def test_4_range_query(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        results = query(f"""
            select
                rental_price, location, sqft, $rental_price_confidence, number_of_rooms
            from
                mindsdb.{test_predictor_name} where $select_data_query='select * from test.{test_data_table} limit 3';
        """, as_dict=True)

        print('check result')
        self.assertTrue(len(results) == 3)
        for res in results:
            self.assertTrue(res['rental_price'] is not None and res['rental_price'] != 'None')
            self.assertTrue(res['location'] is not None and res['location'] != 'None')
            self.assertIsInstance(res['$rental_price_confidence'], float)

    def test_5_delete_predictor_by_command(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        query(f"""
            insert into mindsdb.commands values ('delete predictor {test_predictor_name}');
        """)

        print(f'Predictor {test_predictor_name} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(test_predictor_name not in models)

        print(f'Test predictor table not exists')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x[0] for x in mindsdb_tables]
        self.assertTrue(test_predictor_name not in mindsdb_tables)

    def test_6_insert_predictor_again(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        self.test_2_insert_predictor()

    def test_7_delete_predictor_by_delete_statement(self):
        print(f'\nExecuting {inspect.stack()[0].function}')
        query(f"""
            delete from mindsdb.predictors where name='{test_predictor_name}';
        """)

        print(f'Predictor {test_predictor_name} not exists')
        models = [x['name'] for x in self.mdb.get_models()]
        self.assertTrue(test_predictor_name not in models)

        print(f'Test predictor table not exists')
        mindsdb_tables = query('show tables from mindsdb')
        mindsdb_tables = [x[0] for x in mindsdb_tables]
        self.assertTrue(test_predictor_name not in mindsdb_tables)


def wait_mysql(timeout):
    global config
    m = Mariadb(config)

    start_time = time.time()

    connected = m.check_connection()
    while connected is False and (time.time() - start_time) < timeout:
        time.sleep(2)
        connected = m.check_connection()

    return connected

def is_port_in_use(port_num):
    portsinuse = []
    conns = psutil.net_connections()
    portsinuse = [x.laddr[1] for x in conns if x.status == 'LISTEN']
    portsinuse.sort()
    return int(port_num) in portsinuse

def wait_port(port_num, timeout):
    start_time = time.time()

    in_use = is_port_in_use(port_num)
    while in_use is False and (time.time() - start_time) < timeout:
        time.sleep(2)
        in_use = is_port_in_use(port_num)

    return in_use


if __name__ == "__main__":
    maria_sp = subprocess.Popen(
        ['./cli.sh', 'mariadb'],
        cwd=pathlib.Path(__file__).parent.absolute().joinpath('../docker/').resolve(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    atexit.register(maria_sp.terminate)
    maria_ready = wait_mysql(15)
    sp = subprocess.Popen(
        ['python3', '-m', 'mindsdb', '--api', 'mysql', '--config', TEST_CONFIG],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    atexit.register(sp.terminate)
    port_num = config['api']['mysql']['port']
    api_ready = wait_port(port_num, 15)
    try:
        if maria_ready is False or api_ready is False:
            raise Exception()
        unittest.main()
        print('Tests passed !')
    except Exception as e:
        print('Tests Failed !')
        print(e)
    print('done')
