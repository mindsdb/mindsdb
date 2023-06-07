from .urlcrawl_helpers import get_df_from_query_str, get_all_websites 
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



class CrawlerTable(APITable):

    def select(self, query: ast.Select) -> Response:

        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')
            
            if arg1 == 'url':
                url = arg2
                
                if op == '=':
                    urls = [str(url)]
                elif op == 'in':
                    if type(url) == str:
                        urls = [str(url)]
                    else:
                        urls = url
                else:
                    raise NotImplementedError

            

            
            else:
                pass
        
        limit = None

        if query.limit is not None:
            limit = query.limit.value

       

       

       

        result = get_all_websites(urls, limit, html=False)
        
        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # filter by columns
            result = result[columns]
        return result

        

    def get_columns(self):
        return [
            'url',
            'text_content',
            'error'
        ]

    


class WebHandler(APIHandler):
    """A class for handling crawling content from websites.

    Attributes:
        
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        
        self.api = None
        self.is_connected = True
        crawler = CrawlerTable(self)
        self._register_table('crawler', crawler)
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
   




