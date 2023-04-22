import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions



class StoriesTable(APITable):

    def __init__(self, handler):
        super().__init__(handler)

        self.name = 'stories'
        self.primary_key = 'id'

    def get(self, select=None, where=None, group_by=None, having=None, order_by=None, limit=None):

        if 'id' not in select:
            select.append('id')

        if not where:
            where = []

        if group_by or having or order_by:
            raise NotImplementedError('This method does not support group_by, having, or order_by arguments')

        query_string = f'get_top_stories({where})'
        response = self.handler.native_query(query_string)

        data_frame = response.data_frame

        data_frame = data_frame[select]

        if limit:
            data_frame = data_frame.head(limit)

        return data_frame

class CommentsTable(APITable):

    def __init__(self, handler):
        super().__init__(handler)

        self.name = 'comments'
        self.primary_key = 'id'

    def get(self, select=None, where=None, group_by=None, having=None, order_by=None, limit=None):

        if 'id' not in select:
            select.append('id')

        item_id = None
        for condition in where:
            if condition[0] == 'item_id':
                item_id = condition[2]
                break

        if item_id is None:
            raise ValueError("An 'item_id' must be provided in the 'where' condition")

        if group_by or having or order_by:
            raise NotImplementedError('This method does not support group_by, having, or order_by arguments')

        query_string = f'get_comments(item_id={item_id})'
        response = self.handler.native_query(query_string)

        data_frame = response.data_frame

        data_frame = data_frame[select]

        if limit:
            data_frame = data_frame.head(limit)

        return data_frame



class PostTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        '''Select data from the post table and return it as a pandas DataFrame.
        Args:
            query (ast.Select): The SQL query to be executed.
        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        '''

        # Get the query parameters from the WHERE clause
        author = None
        score = None
        conditions = self.extract_comparison_conditions(query.where)
        for condition in conditions:
            if condition[1] == 'author':
                author = condition[2]
            elif condition[1] == 'score':
                score = int(condition[2])

        # Retrieve data from the Hacker News API
        url = 'https://hacker-news.firebaseio.com/v0/topstories.json'
        response = requests.get(url)
        ids = response.json()

        result = []
        for post_id in ids:
            post_url = f'https://hacker-news.firebaseio.com/v0/item/{post_id}.json'
            post_response = requests.get(post_url)
            post = post_response.json()

            # Filter by author and score if specified
            if (not author or post.get('by') == author) and (score is None or post.get('score') == score):
                data = {
                    'id': post.get('id'),
                    'title': post.get('title'),
                    'url': post.get('url'),
                    'text': post.get('text'),
                    'author': post.get('by'),
                    'score': post.get('score'),
                    'time': post.get('time'),
                    'descendants': post.get('descendants'),
                }
                result.append(data)

        result = pd.DataFrame(result)
        self.filter_columns(result, query)
        return result


    def get_columns(self):
        '''Get the list of column names for the post table.
        Returns:
            list: A list of column names for the post table.
        '''
        return [
            'id',
            'title',
            'url',
            'text',
            'author',
            'score',
            'time',
            'descendants',
        ]

    def filter_columns(self, result: pd.DataFrame, query: ast.Select = None):
        columns = []
        if query is not None:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    columns = self.get_columns()
                    break
                elif isinstance(target, ast.Identifier):
                    columns.append(target.parts[-1])
                else:
                    raise NotImplementedError
        else:
            columns = self.get_columns()

        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            result = result[columns]

        if query is not None and query.limit is not None:
            return result.head(query.limit.value)

        return result
