import os
import praw

import datetime as dt
import ast
from collections import defaultdict

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

log = get_log()

class RedditTable(APITable):
    def __init__(self, handler):
        self.handler = handler
        self.reddit = self.handler.connect()

    def select(self, query: ast.Select) -> Response:
        subreddit = self.reddit.subreddit(query.from_table)

        conditions = extract_comparison_conditions(query.where)

        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'created_utc':
                date = dt.datetime.fromtimestamp(int(arg2))
                if op == '>':
                    params['after'] = int(date.timestamp())
                elif op == '<':
                    params['before'] = int(date.timestamp())
                else:
                    raise NotImplementedError
                continue

            if op != '=':
                raise NotImplementedError

            params[arg1] = arg2

        if query.limit is not None:
            params['limit'] = query.limit.value

        if 'sort' in params:
            if params['sort'] == 'top':
                params['sort_type'] = 'top'
            elif params['sort'] == 'new':
                params['sort_type'] = 'new'
            elif params['sort'] == 'hot':
                params['sort_type'] = 'hot'
            else:
                raise ValueError('Invalid sort value')

            del params['sort']

        result = []
        for post in subreddit.search(**params):
            post_data = defaultdict(lambda: None)
            post_data.update({
                'id': post.id,
                'created_utc': post.created_utc,
                'title': post.title,
                'selftext': post.selftext,
                'score': post.score,
                'author': post.author.name,
                'subreddit': post.subreddit.display_name,
                'num_comments': post.num_comments,
                'permalink': f'https://www.reddit.com{post.permalink}'
            })
            result.append(post_data)

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
            for col in set(columns) & set(result[0].keys()) ^ set(columns):
                for post_data in result:
                    post_data[col] = None

            # filter by columns
            result = [{col: post_data[col] for col in columns} for post_data in result]

        return result

    def get_columns(self):
        return [
            'id',
            'created_utc',
            'title',
            'selftext',
            'score',
            'author',
            'subreddit',
            'num_comments',
            'permalink'
        ]
    
class RedditHandler(APIHandler):
    """A class for handling connections and interactions with the Reddit API.

    Attributes:
        client_id (str): The client ID for the Reddit app.
        client_secret (str): The client secret for the Reddit app.
        username (str): The Reddit account's username.
        password (str): The Reddit account's password.
        api (praw.Reddit): The `praw.Reddit` object for interacting with the Reddit API.

    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = Config().get('reddit_handler', {})
        for k in ['client_id', 'client_secret', 'username', 'password']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'REDDIT_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'REDDIT_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

        posts = RedditTable(self)
        self._register_table('posts', posts)

    def connect(self):
        """Authenticate with the Reddit API using the client ID, client secret, username, and password."""

        if self.is_connected is True:
            return self.api

        self.api = praw.Reddit(
            client_id=self.connection_args['client_id'],
            client_secret=self.connection_args['client_secret'],
            username=self.connection_args['username'],
            password=self.connection_args['password'],
            user_agent=f"{self.connection_args['username']}-api-handler"
        )

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            api = self.connect()
            api.user.me()

            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Reddit API: {e}'
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_reddit_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_reddit_api(self, method_name: str = None, params: dict = None):
        api = self.connect()
        try:
            if method_name is not None:
                if params is not None:
                    response = api.request(method_name, params)
                else:
                    response = api.request(method_name)
                return response
            else:
                raise ValueError("Method name cannot be None")
        except RequestException as e:
            print(f"An error occurred while calling the Reddit API: {e}")

