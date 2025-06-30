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
import re
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict
logger = log.getLogger(__name__)

class DuckDuckGoSearchTable(APITable):
    def __init__(self, handler: 'DuckDuckGoHandler'):
        super().__init__(handler)
        self.handler = handler

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Performs a DuckDuckGo search based on the provided SQL query and returns the results as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query object.

        Returns:
            pd.DataFrame: The search results as a pandas DataFrame.
        """
        
        match = re.search(r"from\s+my_duckduckgo\s*\(\s*['\"](.*?)['\"]\s*\)", str(query), re.IGNORECASE)
        if match:
            search_query = match.group(1)
        else:
            error_message = "Invalid SQL query format"
            logger.error(error_message)
            raise ValueError(error_message)

        params = {
            "q": search_query,
            "format": "json",
            "no_html": "1",
            "no_redirect": "1",
            "skip_disambig": "1"
        }

        try:
           
            result = self.handler.call_duckduckgo_api(params=params)
            if result is not None:
                data_frame = pd.json_normalize(result)
                return data_frame
            else:
                error_message = "API request failed"
                logger.error(error_message)
                raise ValueError(error_message)
        except Exception as e:
            error_message = str(e)
            logger.error(error_message)
            raise ValueError(error_message)

    def get_columns(self) -> list:
        """
        Gets all columns to be returned in the pandas DataFrame response.
        """
        return [
            "Abstract",
            "AbstractSource",
            "AbstractText",
            "AbstractURL",
            "Answer",
            "AnswerType",
            "Definition",
            "DefinitionSource",
            "DefinitionURL",
            "Heading",
            "Image",
            "Redirect",
            "RelatedTopics",
        ]

class DuckDuckGoHandler(APIHandler):
    def __init__(self, name: str = None, **kwargs):
        super().__init__(name)
        self.api_key = None
        args = kwargs.get('connection_data', {})
        if 'api_key' in args:
            self.api_key = args['api_key']

        duckduckgo_search_table = DuckDuckGoSearchTable(self)
        self._register_table("my_duckduckgo", duckduckgo_search_table)
    
    def connect(self) -> typing.Union[str, None]:
        self.is_connected = True if self.is_connected else False
        return self.api_key or None  

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        if self.api_key is None:
            response.error_message = "API key is missing"
        else:
            response.success = True
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
        logger.info(f"Performing search for '{query}'")  

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
                logger.error(error_message)  
                response = Response(RESPONSE_TYPE.ERROR, error_message=error_message)
        except Exception as e:
            error_message = str(e)
            logger.error(error_message)  
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
