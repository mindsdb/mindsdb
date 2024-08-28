from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.utilities.date_utils import interval_str_to_duration_ms, utc_date_str_to_timestamp_ms
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from typing import Dict, List

import pandas as pd
import time
import requests
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinancialModelingTradesTable(APITable):
    def _get_daily_endpoint_params_from_conditions(self, conditions: List) -> Dict:
        params = {}
        for op, arg1, arg2 in conditions: 
            if arg1 == 'symbol':
                if op != '=':
                    raise NotImplementedError
                params['symbol'] = arg2
            if arg1 == "from_date":
                if op != '=':
                    raise NotImplementedError
                params['from'] = arg2
            if arg1 == "to_date":
                if op != '=':
                    raise NotImplementedError
                params['to'] = arg2
            

        return params

    # def select(self, query: ast.Select) -> pd.DataFrame:
    #     """Selects data from the FinancialModeling API and returns it as a pandas DataFrame.
        
    #     Returns dataframe representing the FinancialModeling API results.

    #     Args:
    #         query (ast.Select): Given SQL SELECT query
    #     """
    #     conditions = extract_comparison_conditions(query.where)
    #     params = self._get_daily_endpoint_params_from_conditions(conditions)
    #     print(conditions)
    #     print("QUERRYYYYY")
    #     print(query)
    #     if query.limit and query.limit.value:
    #         limit_value = query.limit.value
    #         params['limit'] = limit_value

    #     daily_chart_table = self.get_daily_chart(
    #             params=params
    #     )
        
    #     return daily_chart_table


    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the FinancialModeling API and returns it as a pandas DataFrame.
        
        Returns dataframe representing the FinancialModeling API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        logger.info("Entering the select method")
        
        # Extract conditions from the query's WHERE clause
        conditions = extract_comparison_conditions(query.where)
        logger.info(f"Conditions extracted: {conditions}")
        
        # Get API endpoint parameters from conditions
        params = self._get_daily_endpoint_params_from_conditions(conditions)
        logger.info(f"Params generated for API call: {params}")

        logger.info("Executing query")
        logger.info(f"Query details: {query}")

        # Handle the query limit
        if query.limit and query.limit.value:
            limit_value = query.limit.value
            params['limit'] = limit_value
            logger.info(f"Applied limit to query: {limit_value}")

        try:
            # Fetch daily chart data using the FinancialModeling API
            daily_chart_table = self.get_daily_chart(params=params)
            logger.info("Data retrieved successfully from the FinancialModeling API")
            
            return daily_chart_table
        
        except AttributeError as e:
            logger.error("AttributeError encountered during query execution", exc_info=True)
            raise
        
        except Exception as e:
            logger.error("Unexpected error during query execution", exc_info=True)
            raise
        
    def get_daily_chart(self, params: Dict = None) -> pd.DataFrame:  
        base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"

        if 'symbol' not in params:
            raise ValueError('Missing "symbol" param')
        symbol = params['symbol']
        params.pop('symbol')

        limitParam = False
        limit = 0
        if 'limit' in params:
            limit = params['limit']
            params.pop('limit')
            limitParam = True

        url = f"{base_url}{symbol}" #https://financialmodelingprep.com/api/v3/historical-price-full/<symbol>
        param = {'apikey': self.api_key, **params}

        response = requests.get(url, param)
        historical_data = response.json()
        historical = historical_data.get("historical")
        

        if limitParam:
            return pd.DataFrame(historical).head(limit)
        
        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                historical
            )
        )

        if historical:
            return pd.DataFrame(historical)
        else:
            return pd.DataFrame() 

    def get_daily_chart(self, params: Dict = None) -> pd.DataFrame:  
        base_url = "https://financialmodelingprep.com/api/v3/historical-price-full/"

        if 'symbol' not in params:
            raise ValueError('Missing "symbol" param')
        symbol = params['symbol']
        params.pop('symbol')

        limitParam = False
        limit = 0
        if 'limit' in params:
            limit = params['limit']
            params.pop('limit')
            limitParam = True

        url = f"{base_url}{symbol}" #https://financialmodelingprep.com/api/v3/historical-price-full/<symbol>
        param = {'apikey': self.api_key, **params}

        response = requests.get(url, param)
        historical_data = response.json()
        historical = historical_data.get("historical")
        

        if limitParam:
            return pd.DataFrame(historical).head(limit)
        
        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                historical
            )
        )

        if historical:
            return pd.DataFrame(historical)
        else:
            return pd.DataFrame() 

    