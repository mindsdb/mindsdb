from .urlcrawl_helpers import get_df_from_query_str 
import logging

import pandas as pd


from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)



class UrlsHandler(APIHandler):
    """A class for handling crawling content from websites.

    Attributes:
        
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        
        self.api = None
        self.is_connected = True

        # tweets = TweetsTable(self)
        # self._register_table('tweets', tweets)

    

    
    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)
        response.success = True
        
        return response

    def native_query(self, query_string: str = None):
        
        df = get_df_from_query_str(query_string)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def _apply_filters(self, data, filters):
        if not filters:
            return data

        data2 = []
        for row in data:
            add = False
            for op, key, value in filters:
                value2 = row.get(key)
                if isinstance(value, int):
                    # twitter returns ids as string
                    value = str(value)

                if op in ('!=', '<>'):
                    if value == value2:
                        break
                elif op in ('==', '='):
                    if value != value2:
                        break
                elif op == 'in':
                    if not isinstance(value, list):
                        value = [value]
                    if value2 not in value:
                        break
                elif op == 'not in':
                    if not isinstance(value, list):
                        value = [value]
                    if value2 in value:
                        break
                else:
                    raise NotImplementedError(f'Unknown filter: {op}')
                # only if there wasn't breaks
                add = True
            if add:
                data2.append(row)
        return data2

    


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    url = "https://www.lpl.com/join-lpl/managing-your-business/services-and-support.html"  # the website url
    reviewed_urls  = {}
    parsed_links = set()
    get_all_website_links_rec(url, reviewed_urls, 1)
   




