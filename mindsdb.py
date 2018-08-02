import sqlite3
import pandas
from libs.helpers.sqlite_helpers import *


class MindsDB:

    def __init__(self, file=":memory:"):
        self.conn = sqlite3.connect(file)
        self.conn.create_aggregate("first_value", 1, FirstValueAgg)
        self.conn.create_aggregate("array_agg_json", 2, ArrayAggJSON)

    def addTable(self, ds, as_table):
        ds.df.to_sql(as_table, self.conn, if_exists='replace', index=False)

    def query(self, query):
        cur = self.conn.cursor()
        return cur.execute(query)

    def queryToDF(self, query):
        return pandas.read_sql_query(query, self.conn)


    def learn(self, from_query, predict):
        pass
        # start server