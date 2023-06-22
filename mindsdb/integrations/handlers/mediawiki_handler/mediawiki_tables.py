import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable

from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor

from mindsdb.utilities.log import get_log

logger = get_log("integrations.mediawiki_handler")


class PagesTable(APITable):
    """The MediaWiki Pages Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls MediaWiki pages data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Sendinblue Email Campaigns matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'pages',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        pages_df = pd.json_normalize(self.get_pages(limit=result_limit))

        return pages_df

    def get_columns(self) -> List[str]:
        return pd.json_normalize(self.get_pages(limit=1)).columns.tolist()

    def get_pages(self, title: str = None, page_id: int = None, limit: int = 20):
        query_parts = []

        query_parts.append(f'intitle:{title}') if title is not None else None
        query_parts.append(f'pageid:{page_id}') if page_id is not None else None

        search_query = ' | '.join(query_parts)

        connection = self.handler.connect()

        if search_query:
            return [self.convert_page_to_dict(connection.page(result, auto_suggest=False)) for result in connection.search(search_query, results=limit)]
        else:
            return [self.convert_page_to_dict(connection.page(result, auto_suggest=False)) for result in connection.random(pages=limit)]

    def convert_page_to_dict(self, page):
        result = {}
        attributes = ['pageid', 'title', 'original_title', 'content', 'summary', 'url', 'categories']

        for attribute in attributes:
            try:
                result[attribute] = getattr(page, attribute)
            except KeyError:
                logger.debug(f"Error accessing '{attribute}' attribute. Skipping...")

        return result