import datetime as dt
import tweepy
import ast
from collections import defaultdict

import pandas as pd

from mindsdb.utilities import log

from mindsdb_sql.parser import ast
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


def extract_conditions(binary_op):
    conditions = []

    def _extract_conditions(node, **kwargs):
        if isinstance(node, ast.BinaryOperation):
            op = node.op.lower()
            if op == 'and':
                return
            elif op == 'or':
                raise NotImplementedError
            elif not isinstance(node.args[0], ast.Identifier) or not isinstance(node.args[1], ast.Constant):
                raise NotImplementedError
            conditions.append([op, node.args[0].parts[-1], node.args[1].value])

    query_traversal(binary_op, _extract_conditions)
    return conditions


def parse_date(date_str):
    if isinstance(date_str, dt.datetime):
        return date_str
    date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
    date = None
    for date_format in date_formats:
        try:
            date = dt.datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    if date is None:
        raise ValueError(f"Can't parse date: {date_str}")
    return date


class TweetsTable(APITable):
    
    def select(self, query: ast.Select) -> Response:

        conditions = extract_conditions(query.where)

        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'created_at':
                if op == '>':
                    params['start_time'] = parse_date(arg2)
                elif op == '<':
                    params['end_time'] = parse_date(arg2)
                else:
                    raise NotImplementedError
                continue

            if op != '=':
                raise NotImplementedError

            params[arg1] = arg2

        if query.limit is not None:
            params['max_results'] = query.limit.value

        params['expansions'] = ['author_id']
        params['tweet_fields'] = ['created_at']
        params['user_fields'] = ['name', 'username']

        if 'query' not in params:
            # search not works without query, use 'mindsdb'
            params['query'] = 'mindsdb'

        result = self.handler.call_twitter_api(
            method_name='search_recent_tweets',
            params=params
        )

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
        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            result = result[columns]
        return result

    def get_columns(self):
        return [
            'text',
            'id',
            'author_id',
            'created_at',
            'edit_history_tweet_ids',
            'author_username',
            'author_name',

            # 'conversation_id',
            # 'hashtags',
            # 'cashtags',
            # 'links',
            # 'mentions',
            # 'media',
            # 'lang',
            # 'context',
            # 'entities',
            # 'place',
            # 'place_country',
            # 'place_geo_coordinates'
            # 'attachments',
            # 'public_metrics'
        ]

    def insert(self, query:ast.Insert):
        # https://docs.tweepy.org/en/stable/client.html#tweepy.Client.create_tweet
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))
            self.handler.call_twitter_api('create_tweet', params)

class TwitterHandler(APIHandler):
    """A class for handling connections and interactions with the Twitter API.

    Attributes:
        bearer_token (str): The consumer key for the Twitter app.
        api (tweepy.API): The `tweepy.API` object for interacting with the Twitter API.

    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        for k in ['bearer_token', 'consumer_key', 'consumer_secret',
                  'access_token', 'access_token_secret', 'wait_on_rate_limit']:
            if k in args:
                self.connection_args[k] = args[k]

        self.api = None
        self.is_connected = False

        tweets = TweetsTable(self)
        self._register_table('tweets', tweets)

    def connect(self):
        """Authenticate with the Twitter API using the API keys and secrets stored in the `consumer_key`, `consumer_secret`, `access_token`, and `access_token_secret` attributes."""

        if self.is_connected is True:
            return self.api

        self.api = tweepy.Client(**self.connection_args)

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            api = self.connect()

            # call get_user with unknown id.
            #   it raises an error in case if auth is not success and returns not-found otherwise
            #   api.get_me() is not exposed for OAuth 2.0 App-only authorisation
            api.get_user(id=1)

            response.success = True

        except tweepy.Unauthorized as e:
            log.logger.error(f'Error connecting to Twitter api: {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_twitter_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_twitter_api(self, method_name:str = None, params:dict = None):

        # method > table > columns
        expansions_map = {
            'search_recent_tweets': {
                'users': ['author_id'],
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
        while True:
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

            resp = method(**params)

            if hasattr(resp, 'includes'):
                for table, records in resp.includes.items():
                    includes[table].extend([r.data for r in records])

            if isinstance(resp.data, list):
                data.extend([r.data for r in resp.data])
            else:
                if isinstance(resp.data, dict):
                    data.append(resp.data)
                break

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
                df_ref = pd.DataFrame(records).drop_duplicates()

                if table not in expansions:
                    continue

                for col_id in expansions[table]:
                    col = col_id[:-3] # cut _id
                    if col_id not in df.columns:
                        continue

                    col_map = {
                        col_ref: f'{col}_{col_ref}'
                        for col_ref in df_ref.columns
                    }
                    df_ref2 = df_ref.rename(columns=col_map)

                    df = df.merge(df_ref2, on=col_id, how='left')

        return df

