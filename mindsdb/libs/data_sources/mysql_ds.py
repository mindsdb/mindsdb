import pandas as pd
import MySQLdb

from mindsdb.libs.data_types.data_source import DataSource


class MySqlDS(DataSource):

    def _setup(self, query=None, host='localhost', user='root', password='', database='mysql', port=3306, table=None):

        if query is None:
            query = f'SELECT * FROM {table}'

        con = MySQLdb.connect(host, user, password, database, port=port)
        df = pd.read_sql(query, con=con)
        con.close()

        col_map = {}
        for col in df.columns:
            col_map[col] = col

        return df, col_map

if __name__ == "__main__":
    from mindsdb import Predictor

    HOST = 'localhost'
    USER = 'root'
    PASSWORD = ''
    DATABASE = 'mysql'
    PORT = 3306

    con = MySQLdb.connect(HOST, USER, PASSWORD, DATABASE)
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS test_mindsdb')
    cur.execute('CREATE TABLE test_mindsdb(col_1 Text, col_2 BIGINT, col_3 BOOL)')
    for i in range(0,200):
        cur.execute(f'INSERT INTO test_mindsdb VALUES ("This is tring number {i}", {i}, {i % 2 == 0})')
    con.commit()
    con.close()

    mysql_ds = MySqlDS(table='test_mindsdb', host=HOST, user=USER, password=PASSWORD, database=DATABASE, port=PORT)
    assert(len(mysql_ds._df) == 200)

    mdb = Predictor(name='analyse_dataset_test_predictor')
    mdb.analyse_dataset(from_data=mysql_ds)
