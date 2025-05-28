import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable

from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor

from mindsdb.utilities import log

logger = log.getLogger(__name__)


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

        title, page_id = None, None
        for condition in where_conditions:
            if condition[1] == 'title':
                if condition[0] != '=':
                    raise ValueError(f"Unsupported operator '{condition[0]}' for column '{condition[1]}' in WHERE clause.")
                title = condition[2]
            elif condition[1] == 'pageid':
                if condition[0] != '=':
                    raise ValueError(f"Unsupported operator '{condition[0]}' for column '{condition[1]}' in WHERE clause.")
                page_id = condition[2]
            else:
                raise ValueError(f"Unsupported column '{condition[1]}' in WHERE clause.")

        pages_df = pd.json_normalize(self.get_pages(title=title, page_id=page_id, limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            pages_df,
            selected_columns,
            [],
            order_by_conditions
        )
        pages_df = select_statement_executor.execute_query()

        return pages_df

    def get_columns(self) -> List[str]:
        return ['pageid', 'title', 'original_title', 'content', 'summary', 'url', 'categories']

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
        attributes = self.get_columns()

        for attribute in attributes:
            try:
                result[attribute] = getattr(page, attribute)
            except KeyError:
                logger.debug(f"Error accessing '{attribute}' attribute. Skipping...")

        return result
