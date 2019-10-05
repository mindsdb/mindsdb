import os

import psycopg2

from mindsdb import Predictor, PostgresDS


con = psycopg2.connect(user='postgres', dbname='postgres')
cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS test_mindsdb')
cur.execute('CREATE TABLE test_mindsdb(col_1 Text, col_2 BIGINT, col_3 BOOL)')
for i in range(0,5000):
    cur.execute(f'INSERT INTO test_mindsdb VALUES (\'This is tring number {i}\', {i}, {i % 2 == 0})')
con.commit()
con.close()

mdb = Predictor(name='analyse_dataset_test_predictor')
pg_ds = PostgresDS(query="SELECT * FROM test_mindsdb")
assert(len(pg_ds._df) == 5000)
mdb.analyse_dataset(from_data=pg_ds)
