from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast
import pandas as pd


class WebzBaseAPITable(APITable):

    ENDPOINT = None
    SORTABLE_COLUMNS = []

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

        if query.order_by:
            if len(query.order_by) > 1:
                raise ValueError("Unsupported to order by multiple fields")
            order_item = query.order_by[0]
            # make sure that column is sortable
            if order_item.field.parts[0] not in type(self).SORTABLE_COLUMNS:
                raise ValueError(f"Order by unknown column {order_item.field.parts[0]}")
            params.update({
                'sort': order_item.field.parts[0],
                'order': order_item.direction.lower()
            })

        if query.limit is not None:
            params['size'] = query.limit.value
        result = self.handler.call_webz_api(
            method_name=type(self).ENDPOINT,
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
    SORTABLE_COLUMNS = [
        'crawled',
        'relevancy',
        'social.facebook.likes',
        'social.facebook.shares',
        'social.facebook.comments',
        'social.gplus.shares',
        'social.pinterest.shares',
        'social.linkedin.shares',
        'social.stumbledupon.shares',
        'social.vk.shares',
        'replies_count',
        'participants_count',
        'performance_score',
        'published',
        'thread.published',
        'domain_rank',
        'ord_in_thread',
        'rating'
    ]


class WebzReviewsTable(WebzBaseAPITable):
    """ To interact with structured reviews data from hundreds of review sites,
    provided through the Webz.IO API.

    """

    ENDPOINT = 'reviewFilter'    
    SORTABLE_COLUMNS = [
        'crawled',
        'relevancy',
        'reviews_count',
        'reviewers_count',
        'spam_score',
        'domain_rank',
        'ord_in_thread',
        'rating'
    ]
