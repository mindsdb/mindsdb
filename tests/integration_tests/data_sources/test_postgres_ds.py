import datetime
import logging
import pg8000
from mindsdb import Predictor
from mindsdb.libs.data_sources.postgres_ds import PostgresDS


def test_postgres_ds():
    HOST = 'localhost'
    USER = 'postgres'
    PASSWORD = ''
    DBNAME = 'postgres'
    PORT = 5432

    con = pg8000.connect(database=DBNAME, user=USER, password=PASSWORD,
                         host=HOST, port=PORT)
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS test_mindsdb')
    cur.execute(
        'CREATE TABLE test_mindsdb(col_1 Text, col_2 Int,  col_3 Boolean, col_4 Date, col_5 Int [])')
    for i in range(0, 200):
        dt = datetime.datetime.now() - datetime.timedelta(days=i)
        dt_str = dt.strftime('%Y-%m-%d')
        cur.execute(
            f'INSERT INTO test_mindsdb VALUES (\'String {i}\', {i}, {i % 2 == 0}, \'{dt_str}\', ARRAY [1, 2, {i}])')
    con.commit()
    con.close()

    mysql_ds = PostgresDS(table='test_mindsdb', host=HOST, user=USER,
                          password=PASSWORD, database=DBNAME, port=PORT)
    assert (len(mysql_ds._df) == 200)

    mdb = Predictor(name='analyse_dataset_test_predictor', log_level=logging.ERROR)
    mdb.analyse_dataset(from_data=mysql_ds)
