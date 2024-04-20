import pandas as pd
from typing import Dict

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql

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


            def connect(self): 
                self.is_connected = True

            def native_query(self, query: str = None) -> Response:
                ast = parse_sql(query, dialect='mindsdb')
                return self.query(ast)

            def call_financial_modeling_api(self, endpoint_name: str = None, params: Dict = None) -> pd.DataFrame:
                """Calls the financial modeling API method with the given params.

                Returns results as a pandas DataFrame.

                Args:
                    
                    params (Dict): Params to pass to the API call
                """
                if endpoint_name == 'daily_chart':
                    return self._get_daily_chart(params)
                raise NotImplementedError('Endpoint {} not supported by Financial Modeling API Handler'.format(endpoint_name))
            
            
            

            
            