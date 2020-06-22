import pandas as pd
import mysql.connector

from mindsdb.libs.data_types.data_source import DataSource


class MySqlDS(DataSource):

    def _setup(self, query=None, host='localhost', user='root', password='',
               database='mysql', port=3306, table=None):

        if query is None:
            query = f'SELECT * FROM {table}'

        con = mysql.connector.connect(host=host,
                                  port=port,
                                  user=user,
                                  password=password,
                                  database=database)
        df = pd.read_sql(query, con=con)
        con.close()

        col_map = {}
        for col in df.columns:
            col_map[col] = col

        return df, col_map
