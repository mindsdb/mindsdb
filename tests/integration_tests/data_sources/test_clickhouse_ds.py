import pytest
import os
import requests
from mindsdb import Predictor
from mindsdb.libs.data_sources.clickhouse_ds import ClickhouseDS


def test_clickhouse_ds():
    HOST = os.environ.get('CLICKHOUSE_HOST', 'localhost')
    PORT = os.environ.get('CLICKHOUSE_PORT', 8123)

    clickhouse_url = f'http://{HOST}:{PORT}'
    requests.post(clickhouse_url,
                  data='CREATE DATABASE IF NOT EXISTS test')
    requests.post(clickhouse_url, data='DROP TABLE IF EXISTS test.mock')
    requests.post(clickhouse_url, data="""CREATE TABLE test.mock(
            col1 String
            ,col2 Int64
            ,col3 Array(UInt8)
        ) ENGINE=Memory""")
    requests.post(clickhouse_url,
                  data="""INSERT INTO test.mock VALUES ('a',1,[1,2,3])""")
    requests.post(clickhouse_url,
                  data="""INSERT INTO test.mock VALUES ('b',2,[2,3,1])""")
    requests.post(clickhouse_url,
                  data="""INSERT INTO test.mock VALUES ('c',3,[3,1,2])""")

    clickhouse_ds = ClickhouseDS('SELECT * FROM test.mock ORDER BY col2 DESC LIMIT 2', host=HOST, port=PORT)

    assert (len(clickhouse_ds.df) == 2)
    assert (sum(map(int, clickhouse_ds.df['col2'])) == 5)
    assert (len(list(clickhouse_ds.df['col3'][1])) == 3)
    assert (set(clickhouse_ds.df.columns) == set(['col1', 'col2', 'col3']))

    mdb = Predictor(name='analyse_dataset_test_predictor')
    mdb.analyse_dataset(from_data=clickhouse_ds)
