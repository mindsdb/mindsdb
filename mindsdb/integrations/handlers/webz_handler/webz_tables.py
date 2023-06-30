from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast
import pandas as pd


class WebzBaseAPITable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """ Selects data from the API and returns it as a pandas DataFrame

        Returns dataframe representing the API results.

        Args:
            query (ast.Select): SQL SELECT query

        """
        conditions = extract_comparison_conditions(query.where)        
        params = {}

        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError(f'Unsupported Operator: {op}')
            elif arg1 == 'query':
                params['q'] = arg2
            else:
                raise NotImplementedError(f'Unknown clause: {arg1}')

        if query.limit is not None:
            params['size'] = query.limit.value
        result = self.handler.call_webz_api(
            method_name='filterWebContent',
            params=params
        )

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError(f"Unknown query target {type(target)}")

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            return pd.DataFrame([], columns=columns)

        # add absent columns
        for col in set(columns) & set(result.columns) ^ set(columns):
            result[col] = None

        # filter by columns
        result = result[columns]

        # Rename columns
        for target in query.targets:
            if target.alias:
                result.rename(columns={target.parts[-1]: str(target.alias)}, inplace=True)
        return result

    def get_columns(self) -> List[str]:
        """ Gets all columns to be returned in pandas DataFrame responses

        Returns
            List of columns

        """
        return [
            'language',
            'title',
            'uuid',
            'text',
            'url',
            'author',
            'published',
            'crawled'
        ]


class WebzPostsTable(WebzBaseAPITable):
    """ To interact with structured posts data from news articles, blog posts and online discussions
    provided through the Webz.IO API.

    """

    ENDPOINT = 'filterWebContent'


class WebzReviewsTable(WebzBaseAPITable):
    """ To interact with structured reviews data from hundreds of review sites,
    provided through the Webz.IO API.

    """

    ENDPOINT = 'reviewFilter'    
