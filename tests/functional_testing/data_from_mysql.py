import os

import MySQLdb

from mindsdb import Predictor, MySqlDS


con = MySQLdb.connect("localhost", "root", "", "mysql")
cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS test_mindsdb')
cur.execute('CREATE TABLE test_mindsdb(col_1 Text, col_2 BIGINT, col_3 BOOL)')
for i in range(0,200):
    cur.execute(f'INSERT INTO test_mindsdb VALUES ("This is tring number {i}", {i}, {i % 2 == 0})')
con.commit()
con.close()

mdb = Predictor(name='analyse_dataset_test_predictor')
mysql_ds = MySqlDS(query="SELECT * FROM test_mindsdb")
mdb.analyse_dataset(from_data=s3_ds)
