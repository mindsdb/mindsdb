import pandas as pd
from mindsdb_sql_parser import ast
from pandas import DataFrame

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class VolumesTable(APITable):
    """
    A class for handling the volumes table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all info about the wanted contents of a bookshelf.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the parameters for the request.
        params = {}
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError
            if arg1 == 'q' or arg1 == 'download' or arg1 == 'langRestrict' \
                    or arg1 == 'printType'\
                    or arg1 == 'source' or arg1 == 'partner' \
                    or arg1 == 'showPreorders' or arg1 == 'startIndex':
                params[arg1] = arg2
            elif arg1 == 'filter':
                if arg2 not in ['ebooks', 'free-ebooks', 'full', 'paid-ebooks', 'partial']:
                    raise NotImplementedError
                params[arg1] = arg2
            elif arg1 == 'libraryRestrict':
                if arg2 not in ['my-library', 'no-restrictions']:
                    raise NotImplementedError
                params[arg1] = arg2
            elif arg1 == 'printType':
                if arg2 not in ['all', 'books', 'magazines']:
                    raise NotImplementedError
                params[arg1] = arg2
            elif arg1 == 'projection':
                if arg2 not in ['lite', 'full']:
                    raise NotImplementedError
                params[arg1] = arg2
            else:
                raise NotImplementedError

        # Get the order by from the query.
        if query.order_by is not None:
            if query.order_by[0].value == 'newest':
                params['orderBy'] = 'newest'
            elif query.order_by[0].value == 'relevance':
                params['orderBy'] = 'relevance'
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['maxResults'] = query.limit.value

        # Get the volumes from the Google Books API.
        bookshelves = self.handler.\
            call_application_api(method_name='get_volumes', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(bookshelves) == 0:
            bookshelves = pd.DataFrame([], columns=selected_columns)
        else:
            bookshelves.columns = self.get_columns()
            for col in set(bookshelves.columns).difference(set(selected_columns)):
                bookshelves = bookshelves.drop(col, axis=1)
        return bookshelves

    def get_columns(self) -> list:
        """
        Gets the columns of the table.

        Returns:
            list: List of column names.
        """
        return [
            'kind',
            'id',
            'etag',
            'selfLink',
            'volumeInfo',
            'userInfo'
            'saleInfo',
            'accessInfo',
            'searchInfo',
        ]


class BookshelvesTable(APITable):
    """
    A class for handling the bookshelves table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all info about the wanted bookshelves.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the parameters for the request.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'userId' or arg1 == 'source' or arg1 == 'fields':
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
            elif arg1 == 'shelf':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['minShelf'] = arg2
                elif op == '<':
                    params['maxShelf'] = arg2
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError

        # Get the bookshelves from the Google Books API.
        bookshelves = self.handler.\
            call_application_api(method_name='get_bookshelves', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(bookshelves) == 0:
            bookshelves = pd.DataFrame([], columns=selected_columns)
        else:
            bookshelves.columns = self.get_columns()
            for col in set(bookshelves.columns).difference(set(selected_columns)):
                bookshelves = bookshelves.drop(col, axis=1)
        return bookshelves

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'kind',
            'id',
            'selfLink',
            'title',
            'description',
            'access',
            'updated',
            'created',
            'volumeCount',
            'volumesLastUpdated'
        ]
