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


#TEST_CONFIG = '/home/maxs/dev/mdb/venv/sources/mindsdb/test_config.json'
TEST_CONFIG = '/home/george/mindsdb/etc/config.json'

test_csv = 'tmp.csv'
test_data_table = 'churn'
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

class MariadbInsert(unittest.TestCase):
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

    def test_1_pass(self):
        print(f'Executing {inspect.stack()[0].function}')
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()
