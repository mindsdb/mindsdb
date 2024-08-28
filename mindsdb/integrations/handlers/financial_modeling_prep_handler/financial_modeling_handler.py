import pandas as pd
from typing import Dict
from urllib.parse import urlencode
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers.financial_modeling_prep_handler.financial_modeling_tables import FinancialModelingTradesTable

from urllib.request import urlopen
from mindsdb.utilities import log
import json
import requests
_FINANCIAL_MODELING_URL = 'https://financialmodelingprep.com/api/v3/'

logger = log.getLogger(__name__)

class FinancialModelingHandler(APIHandler):

    name = "financial_modeling"

    def __init__(self, name, connection_data: dict,  **kwargs):
        super().__init__(name)

        self.api_key = None
        self.connection_data = connection_data
        if "api_key" not in connection_data:
            raise Exception(
                "FINANCIAL_MODELING engine requires an API key. Retrieve an API key from https://site.financialmodelingprep.com/developer. See financial_modeling_prep_handler/README.MD on how to include API key in query."
            )
        self.api_key = connection_data['api_key']
        self.client = None
        self.is_connected = False

        daily_chart_table = FinancialModelingTradesTable(self) 
        self._register_table('daily_chart_table', daily_chart_table)

    def connect(self) -> StatusResponse: 
        self.is_connected = True
        return StatusResponse(success = True)
        # base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"
        # return 
    
    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        return StatusResponse(success = True)


    def call_financial_modeling_api(self, endpoint_name: str = None, params: Dict = None) -> pd.DataFrame:
        """Calls the financial modeling API method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            
            params (Dict): Params to pass to the API call
        """
        
        if endpoint_name == 'daily_chart':
            return self.get_daily_chart(params)
        raise NotImplementedError('Endpoint {} not supported by Financial Modeling API Handler'.format(endpoint_name))
    
