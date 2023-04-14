import os
import praw
import json
import pandas as pd
from praw.models import MoreComments
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse ,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
class RedditPostsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where)

        subreddits = []
        search_keys = []
        post_limit = 10

        for op, arg1, arg2 in conditions:
            if op == '=':
                if arg1 == 'subreddit':
                    subreddits.append(arg2)
                elif arg1 == 'search_key':
                    search_keys.append(arg2)
                elif arg1 == 'post_limit':
                    post_limit = int(arg2)

        post_list = []

        for sr in subreddits:
            ml_subreddit = self.handler.reddit.subreddit(sr)

            for kw in search_keys:
                relevant_posts = ml_subreddit.search(kw, limit=post_limit)

                for post in relevant_posts:
                    post_dict = {
                        'title': post.title,
                        'author': post.author.name,
                        'created_utc': post.created_utc,
                        'selftext': post.selftext,
                        'comments': []
                    }

                    for top_level_comment in post.comments:
                        if isinstance(top_level_comment, MoreComments):
                            continue
                        post_dict['comments'].append(top_level_comment.body)

                    post_list.append(post_dict)

        return pd.DataFrame(post_list)

class RedditHandler(APIHandler):

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = {'client_id': 'YOUR_CLIENT_ID', 'client_secret': 'YOUR_CLIENT_SECRET', 'user_agent': 'YOUR_USER_AGENT'}

        for k in ['client_id', 'client_secret', 'user_agent']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'REDDIT_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'REDDIT_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.reddit = None

        reddit_posts = RedditPostsTable(self)
        self._register_table('reddit_posts', reddit_posts)

    def connect(self):
        if self.reddit is not None:
            return self.reddit

        self.reddit = praw.Reddit(
            client_id=self.connection_args['client_id'],
            client_secret=self.connection_args['client_secret'],
            user_agent=self.connection_args['user_agent']
        )

        return self.reddit

    def check_connection(self):
        try:
            reddit = self.connect()
            reddit.user.me()
            return HandlerStatusResponse(success=True, error_message=None)
        except Exception as e:
            print(f"Error connecting to Reddit: {e}")
            return HandlerStatusResponse(success=False, error_message=f"Error connecting to Reddit: {e}")
