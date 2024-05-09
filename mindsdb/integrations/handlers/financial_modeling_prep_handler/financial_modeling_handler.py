import pandas as pd
from typing import Dict
from urllib.parse import urlencode
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.integrations.handlers.financial_modeling_prep_handler.financial_modeling_tables import FinancialModelingTradesTable

from urllib.request import urlopen
from mindsdb.utilities import log
from mindsdb_sql import parse_sql
import certifi
import json
import requests
# https://site.financialmodelingprep.com/developer/docs/daily-chart-charts
#To authorize your requests, add ?apikey= ----- at the end of every request.

_FINANCIAL_MODELING_URL = 'https://financialmodelingprep.com/api/v3/'

logger = log.getLogger(__name__)

class FinancialModelingHandler(APIHandler):
    def __init__(self, name=None, **kwargs):
            super().__init__(name)

            self.api_key = None

            args = kwargs.get('connection_data', {})
            if 'api_key' in args:
                self.api_key = args['api_key']
            
            self.client = None
            self.is_connected = False

            daily_chart_table = FinancialModelingTradesTable(self) #table is instance of table class
            self._register_table('daily_chart_table', daily_chart_table)

            def connect(self): 
                self.is_connected = True

            def native_query(self, query: str = None) -> Response:
                ast = parse_sql(query, dialect='mindsdb')
                return self.query(ast)
            

            #include api_key in params for now
            def get_daily_chart(self, params: Dict = None) -> pd.DataFrame:  
                base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"

                if 'symbol' not in params:
                    raise ValueError('Missing "symbol" param')
                symbol = params['symbol']
                params.pop('symbol')

                url = f"{base_url}{symbol}" #https://financialmodelingprep.com/api/v3/historical-price-full/<symbol>
                param = {'apikey': self.api_key, **params}

                response = requests.get(url, param)
                historical_data = response.json()

                return historical_data
    
    
            def call_financial_modeling_api(self, endpoint_name: str = None, params: Dict = None) -> pd.DataFrame:
                """Calls the financial modeling API method with the given params.

                Returns results as a pandas DataFrame.

                Args:
                    
                    params (Dict): Params to pass to the API call
                """
                if endpoint_name == 'daily_chart':
                    return self.get_daily_chart(params)
                raise NotImplementedError('Endpoint {} not supported by Financial Modeling API Handler'.format(endpoint_name))
            


# base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"
# symbol = "AAPL"
# params = {
#     "from": "2023-10-10",
#     "to": "2023-12-10",
#     "serietype": "line"
# }

# url = f"{base_url}{symbol}"
# response = requests.get(url, params=params)

# if response.status_code == 200:
#     data = response.json()
#     # Process the data here
# else:
#     print("Failed to retrieve data:", response.status_code)
