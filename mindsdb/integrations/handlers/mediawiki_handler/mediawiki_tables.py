import mediawikiapi
import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable

from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor


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

        return

    def get_columns(self) -> List[str]:
        return pd.json_normalize(self.get_pages(limit=1)).columns.tolist()

    def get_pages(self, search_term: str = None, title: str = None, page_id: int = None, limit: int = 20):
        if search_term:
            pages = self.get_pages_by_search_term(search_term, limit)
        elif title:
            pages = [self.get_page_by_title(title)]
        elif page_id:
            pages = [self.get_page_by_id(page_id)]

        return [self.convert_page_to_dict(page) for page in pages]

    def get_pages_by_search_term(self, search_term: str, limit: int = 20):
        connection = self.handler.connect()

        pages = []
        for result in connection.search(search_term, results=limit):
            pages.append(connection.page(result, auto_suggest=False))

        return pages

    def get_page_by_title(self, title: str):
        connection = self.handler.connect()

        page = connection.page(title, auto_suggest=False)

        return page

    def get_page_by_id(self, page_id: int):
        connection = self.handler.connect()

        page = connection.page(pageid=page_id)

        return page

    def convert_page_to_dict(self, page):
        return {
            'pageid': page.pageid,
            'title': page.title,
            'original_title': page.original_title,
            'content': page.content,
            'summary': page.summary,
            'url': page.url,
            'categories': page.categories,
            'coordinates': page.coordinates,
            'images': page.images,
            'references': page.references,
            'section': page.section,
            'sections': page.sections,
            'links': page.links,
            'revision_id': page.revision_id,
            'parent_id': page.parent_id
        }