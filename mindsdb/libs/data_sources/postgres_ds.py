import os

import pandas as pd
import psycopg2

from mindsdb.libs.data_types.data_source import DataSource


class PostgresDS(DataSource):

    def _setup(self, query=None, host='localhost', user='postgres', password='', database='postgres', port=5432, table=None):

        if query is None:
            query = f'SELECT * FROM {table}'

        con = psycopg2.connect(dbname=database, user=user, password=password, host=host, port=port)
        df = pd.read_sql(query, con=con)
        con.close()

        col_map = {}
        for col in df.columns:
            col_map[col] = col

        return df, col_map

if __name__ == "__main__":
    from mindsdb import Predictor

    HOST = 'localhost'
    USER = 'postgres'
    PASSWORD = ''
    DBNAME = 'postgres'
    PORT = 5432

    con = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS test_mindsdb')
    cur.execute('CREATE TABLE test_mindsdb(col_1 Text, col_2 Int, col_3 Boolean)')
    for i in range(0,200):
        cur.execute(f'INSERT INTO test_mindsdb VALUES (\'This is tring number {i}\', {i}, {i % 2 == 0})')
    con.commit()
    con.close()

    mysql_ds = PostgresDS(table='test_mindsdb', host=HOST, user=USER, password=PASSWORD, database=DBNAME, port=PORT)
    assert(len(mysql_ds._df) == 200)

    mdb = Predictor(name='analyse_dataset_test_predictor')
    mdb.analyse_dataset(from_data=mysql_ds)