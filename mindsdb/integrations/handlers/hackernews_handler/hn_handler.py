import re
import os
import datetime as dt
import ast
import time
from collections import defaultdict
import pytz
import io
import requests

import pandas as pd
from yarl import URL

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
from .hn_table import StoriesTable, CommentsTable
class HackerNewsHandler(APIHandler):
    """A class for handling connections and interactions with the Hacker News API."""

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        self.base_url = 'https://hacker-news.firebaseio.com/v0'

        stories = StoriesTable(self)
        self._register_table('stories', stories)

        comments = CommentsTable(self)
        self._register_table('comments', comments)

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(True)
        return response

    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_hackernews_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_hackernews_api(self, method_name: str = None, params: dict = None, filters: list = None):
        if method_name == 'get_top_stories':
            url = f'{self.base_url}/topstories.json'
            response = requests.get(url)
            data = response.json()
            df = pd.DataFrame(data, columns=['id'])
        elif method_name == 'get_comments':
            item_id = params.get('item_id')
            url = f'{self.base_url}/item/{item_id}.json'
            response = requests.get(url)
            item_data = response.json()
            if 'kids' in item_data:
                comments_data = []
                for comment_id in item_data['kids']:
                    url = f'{self.base_url}/item/{comment_id}.json'
                    response = requests.get(url)
                    comment_data = response.json()
                    comments_data.append(comment_data)
                df = pd.DataFrame(comments_data)
            else:
                df = pd.DataFrame()
        else:
            raise ValueError(f'Unknown method_name: {method_name}')

        return df

