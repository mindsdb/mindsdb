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
        url_passed = False
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')
            
            if arg1 == 'url':
                url = arg2
                url_passed = True

                if op == '=':
                    urls = [str(url)]
                elif op == 'in':
                    if type(url) == str:
                        urls = [str(url)]
                    else:
                        urls = url
                else:
                    raise NotImplementedError(f'url can be url = "someurl", you can also crawl multiple sites, as follows: url IN ("url1", "url2", ..)' )
            
            else:
                pass
        
        if url_passed is False:
            raise NotImplementedError(f'You must specify what url you want to crawl, for example: SELECT * FROM crawl WHERE url IN ("someurl", ..)' )
            
        limit = None

        if query.limit is not None:
            limit = query.limit.value
            if limit < 0:
                limit = None
        if limit is None:
            raise NotImplementedError(f'You must specify a LIMIT which defines how deep to crawl, a LIMIT -1 means that will crawl ALL websites and subwebsites (this can take a while)' )
            

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
    




