import logging

import pandas as pd

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, project_dataframe

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from .urlcrawl_helpers import get_df_from_query_str, get_all_websites, get_all_website_links_rec


class CrawlerTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:

        conditions = extract_comparison_conditions(query.where)

        urls = []
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

        if len(urls) == 0:
            # empty result
            return pd.DataFrame([], columns=self.get_columns())

        result = get_all_websites(urls, limit, html=False)

        result = project_dataframe(result, query.targets, self.get_columns())

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    url = "https://www.lpl.com/join-lpl/managing-your-business/services-and-support.html"  # the website url
    reviewed_urls  = {}
    parsed_links = set()
    get_all_website_links_rec(url, reviewed_urls, 1)
   




