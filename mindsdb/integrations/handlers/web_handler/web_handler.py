import pandas as pd

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, project_dataframe

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.security import is_private_url
from mindsdb.utilities.config import Config

from .urlcrawl_helpers import get_all_websites


class CrawlerTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:

        conditions = extract_comparison_conditions(query.where)
        urls = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError('OR is not supported')

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
                    raise NotImplementedError('Invalid URL format. Please provide a single URL like url = "example.com" or'
                                              'multiple URLs using the format url IN ("url1", "url2", ...)')
            else:
                pass

        if len(urls) == 0:
            raise NotImplementedError(
                'You must specify what url you want to crawl, for example: SELECT * FROM crawl WHERE url = "someurl"')

        if query.limit is None:
            raise NotImplementedError('You must specify a LIMIT which defines the number of pages to crawl')
        limit = query.limit.value

        if limit < 0:
            limit = 0

        config = Config()
        is_cloud = config.get("cloud", False)
        if is_cloud:
            urls = [
                url
                for url in urls
                if not is_private_url(url)
            ]

        result = get_all_websites(urls, limit, html=False)
        if len(result) > limit:
            result = result[:limit]
        # filter targets
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
        crawler = CrawlerTable(self)
        self._register_table('crawler', crawler)
