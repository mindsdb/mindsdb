import requests  

from typing import Dict, Any
from collections import OrderedDict
import pandas as pd
from mindsdb.utilities import log  

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

logger = log.getLogger(__name__)  

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
            error_message = f"API request failed with status code {response.status_code}"
            logger.error(error_message)
            return None

    def native_query(self, query: str) -> Response:
        """
        Perform a search query using DuckDuckGo's API and return the results.

        Args:
            query (str): The search query.

        Returns:
            Response: The response object containing the search results.
        """
        logger.info(f"Performing search for '{query}'")  # Log the search query

        params = {
            "q": query,
            "format": "json",
            "no_html": "1",
            "no_redirect": "1",
            "skip_disambig": "1"
        }

        try:
            result = self.call_duckduckgo_api(params=params)
            if result is not None:
                data_frame = pd.json_normalize(result)
                response = Response(RESPONSE_TYPE.TABLE, data_frame=data_frame)
            else:
                error_message = "API request failed"
                logger.error(error_message)  # Log the error message
                response = Response(RESPONSE_TYPE.ERROR, error_message=error_message)
        except Exception as e:
            error_message = str(e)
            logger.error(error_message)  # Log the exception
            response = Response(RESPONSE_TYPE.ERROR, error_message=error_message)

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
