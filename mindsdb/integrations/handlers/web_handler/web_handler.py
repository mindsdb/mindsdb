import pandas as pd

from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, project_dataframe

from mindsdb.utilities.security import is_private_url
from mindsdb.utilities.config import Config

from .urlcrawl_helpers import get_all_websites


class CrawlerTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Selects data from the provided websites

        Args:
            query (ast.Select): Given SQL SELECT query

        Returns:
            dataframe: Dataframe containing the crawled data

        Raises:
            NotImplementedError: If the query is not supported
        """
        conditions = extract_comparison_conditions(query.where)
        urls = []
        for operator, arg1, arg2 in conditions:
            if operator == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'url':
                if operator in ['=', 'in']:
                    urls = [str(arg2)] if isinstance(arg2, str) else arg2
                else:
                    raise NotImplementedError('Invalid URL format. Please provide a single URL like url = "example.com" or'
                                              'multiple URLs using the format url IN ("url1", "url2", ...)')

        if len(urls) == 0:
            raise NotImplementedError(
                'You must specify what url you want to crawl, for example: SELECT * FROM crawl WHERE url = "someurl"')

        if query.limit is None:
            raise NotImplementedError('You must specify a LIMIT clause which defines the number of pages to crawl')

        limit = query.limit.value

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
        """
        Returns the columns of the crawler table
        """
        return [
            'url',
            'text_content',
            'error'
        ]


class WebHandler(APIHandler):
    """
    Web handler, handling crawling content from websites.
    """
    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        crawler = CrawlerTable(self)
        self._register_table('crawler', crawler)

    def check_connection(self) -> HandlerStatusResponse:
        """
        Checks the connection to the web handler
        @TODO: Implement a better check for the connection

        Returns:
            HandlerStatusResponse: Response containing the status of the connection. Hardcoded to True for now.
        """
        response = HandlerStatusResponse(True)
        return response
