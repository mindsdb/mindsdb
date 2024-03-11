import requests
from typing import Dict, Any
from collections import OrderedDict
import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

class DuckDuckGoHandler(APIHandler):
    def __init__(self, name: str = None, **kwargs):
        super().__init__(name)
        self.api_key = None
        self.is_connected = False

        args = kwargs.get('connection_data', {})
        if 'api_key' in args:
            self.api_key = args['api_key']

    def connect(self) -> bool:
        if self.api_key:
            self.is_connected = True
            return True
        else:
            return False

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            if self.connect():
                response.success = True
            else:
                response.error_message = "API key is missing"
        except Exception as e:
            response.error_message = str(e)

        return response

    def call_duckduckgo_api(self, params: Dict = None) -> Any:
        url = "https://duckduckgo-duckduckgo-zero-click-info.p.rapidapi.com/"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "duckduckgo-duckduckgo-zero-click-info.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

    def native_query(self, query: str) -> Response:
        """
        Perform a search query using DuckDuckGo's API and return the results.
        
        Args:
            query (str): The search query.
            
        Returns:
            Response: The response object containing the search results.
        """
        params = {
            "q": query,
            "format": "json",
            "no_html": "1",
            "no_redirect": "1",
            "skip_disambig": "1"
        }
        
        try:
            result = self.call_duckduckgo_api(params=params)
            # Here we directly use json_normalize to create a DataFrame from the nested JSON response
            # Adjust this based on how you want to present the results
            data_frame = pd.json_normalize(result)
            response = Response(RESPONSE_TYPE.TABLE, data_frame=data_frame)
        except Exception as e:
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        
        return response

connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "DuckDuckGo Zero-Click Info API key.",
        "required": True,
        "label": "API key",
    },
)

connection_args_example = OrderedDict(
    api_key="your_api_key_here",
)