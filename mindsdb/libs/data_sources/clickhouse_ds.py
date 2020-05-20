import pandas as pd
import requests

from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_types.mindsdb_logger import log


class ClickhouseDS(DataSource):

    def _setup(self, query, host='http://localhost', user='default', password=None, port=8123):

        if ' format ' in query.lower():
            err_msg = 'Please refrain from adding a "FROAMT" statement to the query'
            log.error(err_msg)
            raise Exception(err_msg)
        
        query = f'{query} FORMAT JSON'
        log.info(f'Getting data via the query: "{query}""')

        params = {'user': user}
        if password is not None:
            params['password'] = password

        response = requests.post(f'{host}:{port}', data=query, params=params)
        
        try:
            data = response.json()['data']
        except:
            log.error(f'Got an invalid response from the database: {response.text}')
            raise Exception(response.text)

        df = pd.DataFrame(data)
        
        col_map = {}
        for col in df.columns:
            col_map[col] = col

        return df, col_map

if __name__ == "__main__":
    log.info('Starting ClickhouseDS tests !')

    log.info('Inserting data')
    requests.post('http://localhost:8123', data='CREATE DATABASE IF NOT EXISTS test')
    requests.post('http://localhost:8123', data='DROP TABLE IF EXISTS test.mock')
    requests.post('http://localhost:8123', data="""CREATE TABLE test.mock(
        col1 String
        ,col2 Int64
        ,col3 Array(UInt8)
    ) ENGINE=Memory""")
    requests.post('http://localhost:8123', data="""INSERT INTO test.mock VALUES ('a',1,[1,2,3])""")
    requests.post('http://localhost:8123', data="""INSERT INTO test.mock VALUES ('b',2,[2,3,1])""")
    requests.post('http://localhost:8123', data="""INSERT INTO test.mock VALUES ('c',3,[3,1,2])""")


    log.info('Querying data')
    clickhouse_ds = ClickhouseDS('SELECT * FROM test.mock ORDER BY col2 DESC LIMIT 2')

    log.info('Validating data integrity')
    assert(len(clickhouse_ds.df) == 2)
    assert(sum(map(int,clickhouse_ds.df['col2'])) == 5)
    assert(len(list(clickhouse_ds.df['col3'][1])) == 3)
    assert(set(clickhouse_ds.df.columns) == set(['col1','col2','col3']))

    log.info('Finished running ClickhouseDS tests successfully !')


