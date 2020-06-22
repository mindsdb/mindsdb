import pandas as pd
import requests

from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_types.mindsdb_logger import log


class ClickhouseDS(DataSource):

    def _setup(self, query, host='localhost', user='default', password=None, port=8123, protocol='http'):
        if protocol not in ('https', 'http'):
            raise ValueError('Unexpected protocol {}'.fomat(protocol))

        if ' format ' in query.lower():
            err_msg = 'Please refrain from adding a "FROAMT" statement to the query'
            log.error(err_msg)
            raise Exception(err_msg)
        
        query = f'{query.rstrip(" ;")} FORMAT JSON'
        log.info(f'Getting data via the query: "{query}""')

        params = {'user': user}
        if password is not None:
            params['password'] = password

        response = requests.post(f'{protocol}://{host}:{port}', data=query, params=params)
        
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



