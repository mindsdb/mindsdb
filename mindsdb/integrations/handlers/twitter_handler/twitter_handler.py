import re
import os
import datetime as dt
import ast
from collections import defaultdict
import pytz
import io
import requests

import pandas as pd
import tweepy

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

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
    date = date.astimezone(pytz.utc)
    return date


class TweetsTable(APITable):
    
    def select(self, query: ast.Select) -> Response:

        conditions = extract_conditions(query.where)

        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'created_at':
                date = parse_date(arg2)
                if op == '>':
                    # "tweets/search/recent" doesn't accept dates earlier than 7 days
                    if (dt.datetime.now(dt.timezone.utc) - date).days > 7:
                        # skip this condition
                        continue
                    params['start_time'] = date
                elif op == '<':
                    params['end_time'] = date
                else:
                    raise NotImplementedError
                continue

            if op != '=':
                raise NotImplementedError

            params[arg1] = arg2

        if query.limit is not None:
            params['max_results'] = query.limit.value

        params['expansions'] = ['author_id', 'in_reply_to_user_id']
        params['tweet_fields'] = ['created_at', 'conversation_id']
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
            'id',
            'created_at',
            'text',
            'edit_history_tweet_ids',
            'author_id',
            'author_name',
            'author_username',
            'conversation_id',
            'in_reply_to_user_id',
            'in_reply_to_user_name',
            'in_reply_to_user_username'
        ]

    def insert(self, query: ast.Insert):
        # https://docs.tweepy.org/en/stable/client.html#tweepy.Client.create_tweet
        columns = [col.name for col in query.columns]

        insert_params = ('consumer_key', 'consumer_secret', 'access_token', 'access_token_secret')
        for p in insert_params:
            if p not in self.handler.connection_args:
                raise Exception(f'To insert data into Twitter, you need to provide the following parameters when connecting it to MindsDB: {insert_params}')  # noqa

        for row in query.values:
            params = dict(zip(columns, row))

            # split long text over 280 symbols
            max_text_len = 280
            text = params['text']
            if len(text) <= 280:
                # Post image if column media_url is provided, only do this on last tweet
                if 'media_url' in params:
                    media_url = params['media_url']
                    try:
                        # create an in memory file
                        r = requests.get(media_url)
                        inmemoryfile = io.BytesIO(r.content)
                        
                        # upload media to twitter
                        api = self.handler.connect(api_version=1)
                        content_type = r.headers['Content-Type']
                        file_type = content_type.split('/')[-1]
                        media = api.media_upload(filename="somefile.{file_type}".format(file_type=file_type), file=inmemoryfile)
                        del params['media_url']
                        params['media_ids'] = [media.media_id]
                    except ValueError:
                        del params['media_url']
                        log.logger.error(f'Error uploading media to Twitter api: {ValueError}!')
                self.handler.call_twitter_api('create_tweet', params)
                continue

            words = re.split('( )', text)

            messages = []

            text2 = ''
            for word in words:
                if len(text2) + len(word) > max_text_len - 3 - 7:  # 3 is for ..., 7 is for (10/11)
                    messages.append(text2.strip())

                    text2 = ''
                text2 += word

            # the last message
            if text2.strip() != '':
                messages.append(text2.strip())

            len_messages = len(messages)
            for i, text in enumerate(messages):
                if i < len_messages - 1:
                    text += '...'
                else:
                    text += ' '

                text += f'({i + 1}/{len_messages})'

                params['text'] = text
                ret = self.handler.call_twitter_api('create_tweet', params)
                inserted_id = ret.id[0]
                params['in_reply_to_tweet_id'] = inserted_id


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
        handler_config = Config().get('twitter_handler', {})
        for k in ['bearer_token', 'consumer_key', 'consumer_secret',
                  'access_token', 'access_token_secret', 'wait_on_rate_limit']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'TWITTER_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'TWITTER_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

        tweets = TweetsTable(self)
        self._register_table('tweets', tweets)

    def connect(self, api_version=2):
        """Authenticate with the Twitter API using the API keys and secrets stored in the `consumer_key`, `consumer_secret`, `access_token`, and `access_token_secret` attributes."""  # noqa

        if self.is_connected is True:
            return self.api
        # if version 1, do not hold connection in self.api, simply return api object
        if api_version == 1:
            auth = tweepy.OAuthHandler(
                self.connection_args['consumer_key'],
                self.connection_args['consumer_secret']
            )
            auth.set_access_token(
                self.connection_args['access_token'],
                self.connection_args['access_token_secret']
            )
            return tweepy.API(auth)
        
        self.api = tweepy.Client(**self.connection_args)

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

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
                chunk = [r.data for r in resp.data]
            else:
                if isinstance(resp.data, dict):
                    data.append(resp.data)
                if hasattr(resp.data, 'data') and isinstance(resp.data.data, dict):
                    data.append(resp.data.data)
                break

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

