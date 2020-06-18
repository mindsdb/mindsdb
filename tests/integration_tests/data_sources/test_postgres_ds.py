import pytest
import os
import logging
import pg8000
from mindsdb import Predictor
from mindsdb.libs.data_sources.postgres_ds import PostgresDS


def test_postgres_ds():
    HOST = os.environ.get('POSTGRES_HOST', 'localhost')
    USER = os.environ.get('POSTGRES_USER', 'postgres')
    PASSWORD = os.environ.get('POSTGRES_PWD', '')
    DBNAME = os.environ.get('POSTGRES_DB', 'postgres')
    PORT = os.environ.get('POSTGRES_PORT', 5432)

    con = pg8000.connect(database=DBNAME, user=USER, password=PASSWORD,
                         host=HOST, port=PORT)
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS test_mindsdb')
    cur.execute(
        'CREATE TABLE test_mindsdb(col_1 Text, col_2 Int, col_3 Boolean)')
    for i in range(0, 200):
        cur.execute(
            f'INSERT INTO test_mindsdb VALUES (\'This is tring number {i}\', {i}, {i % 2 == 0})')
    con.commit()
    con.close()

    mysql_ds = PostgresDS(table='test_mindsdb', host=HOST, user=USER,
                          password=PASSWORD, database=DBNAME, port=PORT)
    assert (len(mysql_ds._df) == 200)

    mdb = Predictor(name='analyse_dataset_test_predictor', log_level=logging.ERROR)
    mdb.analyse_dataset(from_data=mysql_ds)
