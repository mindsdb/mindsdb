import re
import os
import datetime as dt
import time
from collections import defaultdict
import io
import requests

import pandas as pd
import tweepy

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql_parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class TweetsTable(APITable):

    def select(self, query: ast.Select) -> Response:

        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'created_at':
                date = parse_utc_date(arg2)
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

            elif arg1 == 'query':
                if op == '=':
                    params[arg1] = arg2
                else:
                    NotImplementedError(f'Unknown op: {op}')

            elif arg1 == 'id':
                if op == '>':
                    params['since_id'] = arg2
                elif op == '>=':
                    raise NotImplementedError("Please use 'id > value'")
                elif op == '<':
                    params['until_id'] = arg2
                elif op == '<=':
                    raise NotImplementedError("Please use 'id < value'")
                else:
                    NotImplementedError('Search with "id=" is not implemented')

            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params['max_results'] = query.limit.value

        params['expansions'] = ['author_id', 'in_reply_to_user_id']
        params['tweet_fields'] = ['created_at', 'conversation_id', 'referenced_tweets']
        params['user_fields'] = ['name', 'username']

        if 'query' not in params:
            # search not works without query, use 'mindsdb'
            params['query'] = 'mindsdb'

        result = self.handler.call_twitter_api(
            method_name='search_recent_tweets',
            params=params,
            filters=filters
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
            'in_reply_to_tweet_id',
            'in_retweeted_to_tweet_id',
            'in_quote_to_tweet_id',
            'in_reply_to_user_id',
            'in_reply_to_user_name',
            'in_reply_to_user_username',
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

            # Post image if column media_url is provided, only do this on last tweet
            media_ids = None
            if 'media_url' in params:
                media_url = params.pop('media_url')

                # create an in memory file
                resp = requests.get(media_url)
                img = io.BytesIO(resp.content)

                # upload media to twitter
                api_v1 = self.handler.create_connection(api_version=1)
                content_type = resp.headers['Content-Type']
                file_type = content_type.split('/')[-1]
                media = api_v1.media_upload(filename="img.{file_type}".format(file_type=file_type), file=img)

                media_ids = [media.media_id]

            words = re.split('( )', text)

            messages = []

            text2 = ''
            pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
            for word in words:
                # replace the links in word to string with the length as twitter short url (23)
                word2 = re.sub(pattern, '-' * 23, word)
                if len(text2) + len(word2) > max_text_len - 3 - 7:  # 3 is for ..., 7 is for (10/11)
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
                    # publish media with the last message
                    if media_ids is not None:
                        params['media_ids'] = media_ids

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

    def create_connection(self, api_version=2):
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

        return tweepy.Client(**self.connection_args)

    def connect(self, api_version=2):
        """Authenticate with the Twitter API using the API keys and secrets stored in the `consumer_key`, `consumer_secret`, `access_token`, and `access_token_secret` attributes."""  # noqa

        if self.is_connected is True:
            return self.api

        self.api = self.create_connection()

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
            response.error_message = f'Error connecting to Twitter api: {e}. Check bearer_token'
            logger.error(response.error_message)

        if response.success is True and len(self.connection_args) > 1:
            # not only bearer_token, check read-write mode (OAuth 2.0 Authorization Code with PKCE)
            try:
                api = self.connect()

                api.get_me()

            except tweepy.Unauthorized as e:
                keys = 'consumer_key', 'consumer_secret', 'access_token', 'access_token_secret'
                response.error_message = f'Error connecting to Twitter api: {e}. Check' + ', '.join(keys)
                logger.error(response.error_message)

                response.success = False

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

            logger.debug(f'>>>twitter in: {method_name}({params})')
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
