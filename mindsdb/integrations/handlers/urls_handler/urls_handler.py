from .urlcrawl_helpers import get_all_website_links_rec 
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
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_twitter_api(method_name, params)

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

    def call_twitter_api(self, method_name: str = None, params: dict = None, filters: list = None):

        # method > table > columns
        expansions_map = {
            'search_recent_tweets': {
                'users': ['author_id', 'in_reply_to_user_id'],
            },
            'search_all_tweets': {
                'users': ['author_id'],
            },
        }

        api = self.connect()
        method = getattr(api, method_name)

        # pagination handle

        count_results = None
        if 'max_results' in params:
            count_results = params['max_results']

        data = []
        includes = defaultdict(list)

        max_page_size = 100
        min_page_size = 10
        left = None

        limit_exec_time = time.time() + 60

        if filters:
            # if we have filters: do big page requests
            params['max_results'] = max_page_size

        while True:
            if time.time() > limit_exec_time:
                raise RuntimeError('Handler request timeout error')

            if count_results is not None:
                left = count_results - len(data)
                if left == 0:
                    break
                elif left < 0:
                    # got more results that we need
                    data = data[:left]
                    break

                if left > max_page_size:
                    params['max_results'] = max_page_size
                elif left < min_page_size:
                    params['max_results'] = min_page_size
                else:
                    params['max_results'] = left

            log.logger.debug(f'>>>twitter in: {method_name}({params})')
            resp = method(**params)

            if hasattr(resp, 'includes'):
                for table, records in resp.includes.items():
                    includes[table].extend([r.data for r in records])

            if isinstance(resp.data, list):
                chunk = [r.data for r in resp.data]
            else:
                if isinstance(resp.data, dict):
                    data.append(resp.data)
                if hasattr(resp.data, 'data') and isinstance(resp.data.data, dict):
                    data.append(resp.data.data)
                break

            # unwind columns
            for row in chunk:
                if 'referenced_tweets' in row:
                    refs = row['referenced_tweets']
                    if isinstance(refs, list) and len(refs) > 0:
                        if refs[0]['type'] == 'replied_to':
                            row['in_reply_to_tweet_id'] = refs[0]['id']
                        if refs[0]['type'] == 'retweeted':
                            row['in_retweeted_to_tweet_id'] = refs[0]['id']
                        if refs[0]['type'] == 'quoted':
                            row['in_quote_to_tweet_id'] = refs[0]['id']

            if filters:
                chunk = self._apply_filters(chunk, filters)

            # limit output
            if left is not None:
                chunk = chunk[:left]

            data.extend(chunk)
            # next page ?
            if count_results is not None and hasattr(resp, 'meta') and 'next_token' in resp.meta:
                params['next_token'] = resp.meta['next_token']
            else:
                break

        df = pd.DataFrame(data)

        # enrich
        expansions = expansions_map.get(method_name)
        if expansions is not None:
            for table, records in includes.items():
                df_ref = pd.DataFrame(records)

                if table not in expansions:
                    continue

                for col_id in expansions[table]:
                    col = col_id[:-3]  # cut _id
                    if col_id not in df.columns:
                        continue

                    col_map = {
                        col_ref: f'{col}_{col_ref}'
                        for col_ref in df_ref.columns
                    }
                    df_ref2 = df_ref.rename(columns=col_map)
                    df_ref2 = df_ref2.drop_duplicates(col_id)

                    df = df.merge(df_ref2, on=col_id, how='left')

        return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    url = "https://www.lpl.com/join-lpl/managing-your-business/services-and-support.html"  # the website url
    reviewed_urls  = {}
    parsed_links = set()
    get_all_website_links_rec(url, reviewed_urls, 1)
   




