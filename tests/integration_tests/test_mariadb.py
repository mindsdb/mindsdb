import unittest
import requests
import os
import csv
import time
import inspect
import subprocess

import MySQLdb

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb.utilities.config import Config


TEST_CONFIG = '/home/maxs/dev/mdb/venv/sources/mindsdb/test_config.json'
#TEST_CONFIG = '/home/george/mindsdb/etc/config.json'

test_csv = 'tests/home_rentals.csv'
test_data_table = 'home_rentals'
test_predictor_name = 'test_predictor'

config = Config(TEST_CONFIG)

def query(q):
    con = MySQLdb.connect(
        host=config['integrations']['default_mariadb']['host'],
        port=config['integrations']['default_mariadb']['port'],
        user=config['integrations']['default_mariadb']['user'],
        passwd=config['integrations']['default_mariadb']['password'],
        db='mindsdb'
    )

    cur = con.cursor()
    cur.execute(q)
    resp = cur.fetchall()
    con.commit()
    con.close()
    return resp

class ClickhouseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mdb = MindsdbNative(config)

        if os.path.isfile(test_csv) is False:
            r = requests.get("https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv")
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
            query(f'''
                CREATE TABLE test.{test_data_table} (
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
            with open(test_csv) as f:
                csvf = csv.reader(f)
                i = 0
                for row in csvf:
                    if i > 0:
                        number_of_rooms = int(row[0])
                        number_of_bathrooms = int(row[1])
                        sqft = int(float(row[2].replace(',','.')))
                        location = str(row[3])
                        days_on_market = int(row[4])
                        initial_price = int(row[5])
                        neighborhood = str(row[6])
                        rental_price = int(float(row[7]))
                        query(f'''INSERT INTO test.{test_data_table} VALUES (
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

    def test_1_pass(self):
        print(f'Executing {inspect.stack()[0].function}')
        self.assertTrue(True)

if __name__ == "__main__":
    sp = subprocess.Popen(['python3', '-m', 'mindsdb', '--api', 'mysql', '--config', TEST_CONFIG])
    try:
        time.sleep(12)
        unittest.main()
        print('Tests passed !')
    except:
        print('Tests Failed !')
    finally:
        sp.terminate()
    print('done')
