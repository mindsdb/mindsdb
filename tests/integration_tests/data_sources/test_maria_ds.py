import mysql.connector
import logging
from mindsdb import Predictor
from mindsdb.libs.data_sources.maria_ds import MariaDS


def test_maria_ds():
    HOST = 'localhost'
    USER = 'root'
    PASSWORD = ''
    DATABASE = 'mysql'
    PORT = 4306

    con = mysql.connector.connect(host=HOST,
                                  port=PORT,
                                  user=USER,
                                  password=PASSWORD,
                                  database=DATABASE)
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS test_mindsdb')
    cur.execute('CREATE TABLE test_mindsdb(col_1 Text, col_2 BIGINT, col_3 BOOL)')
    for i in range(0, 200):
        cur.execute(f'INSERT INTO test_mindsdb VALUES ("This is string number {i}", {i}, {i % 2 == 0})')
    con.commit()
    con.close()

    maria_ds = MariaDS(table='test_mindsdb', host=HOST, user=USER,
                       password=PASSWORD, database=DATABASE, port=PORT)
    assert (len(maria_ds._df) == 200)

    mdb = Predictor(name='analyse_dataset_test_predictor', log_level=logging.ERROR)
    mdb.analyse_dataset(from_data=maria_ds)
