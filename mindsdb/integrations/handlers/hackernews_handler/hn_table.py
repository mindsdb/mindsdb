import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from typing import List


class StoriesTable(APITable):
    json_endpoint = "topstories.json"
    columns = ['id', 'time', 'title', 'url', 'score', 'descendants']

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the stories table and return it as a pandas DataFrame.
        Args:
            query (ast.Select): The SQL query to be executed.
        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """
        hn_handler = self.handler

        # Extract the limit value from the SQL query, if it exists
        limit = None
        if query.limit is not None:
            limit = query.limit.value

        df = hn_handler.get_df_from_class(self, limit)

        # Apply any WHERE clauses in the SQL query to the DataFrame
        conditions = extract_comparison_conditions(query.where)
        for condition in conditions:
            if condition[0] == '=' and condition[1] == 'id':
                df = df[df['id'] == int(condition[2])]
            elif condition[0] == '>' and condition[1] == 'time':
                timestamp = int(condition[2])
                df = df[df['time'] > timestamp]

        # Filter the columns in the DataFrame according to the SQL query
        self.filter_columns(df, query)

        return df

    def get_columns(self):
        """Get the list of column names for the stories table.
        Returns:
            list: A list of column names for the stories table.
        """
        return self.columns

    def filter_columns(self, df, query):
        """Filter the columns in the DataFrame according to the SQL query.
        Args:
            df (pandas.DataFrame): The DataFrame to filter.
            query (ast.Select): The SQL query to apply to the DataFrame.
        """
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.value)
        df = df[columns]
        return df


class HNStoriesTable(StoriesTable):
    json_endpoint = "askstories.json"
    columns = ['id', 'time', 'title', 'text', 'score', 'descendants']


class JobStoriesTable(StoriesTable):
    json_endpoint = "jobstories.json"
    columns = ['id', 'time', 'title', 'url', 'score', 'type']


class ShowStoriesTable(StoriesTable):
    json_endpoint = "showstories.json"
    columns = ['id', 'time', 'title', 'text', 'score', 'descendants']


class CommentsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the comments table and return it as a pandas DataFrame.
        Args:
            query (ast.Select): The SQL query to be executed.
        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """
        hn_handler = self.handler

        # Get the limit value from the SQL query, if it exists
        limit = None
        if query.limit is not None:
            limit = query.limit.value

        # Get the item ID from the SQL query
        item_id = None
        conditions = extract_comparison_conditions(query.where)
        for condition in conditions:
            if condition[0] == '=' and condition[1] == 'item_id':
                item_id = condition[2]

        if item_id is None:
            raise ValueError('Item ID is missing in the SQL query')

        # Call the Hacker News API to get the comments for the specified item
        comments_df = hn_handler.call_hackernews_api('get_comments', params={'item_id': item_id})

        # Fill NaN values with 'deleted'
        comments_df = comments_df.fillna('deleted')
        # Filter the columns to those specified in the SQL query
        self.filter_columns(comments_df, query)

        # Limit the number of results if necessary
        if limit is not None:
            comments_df = comments_df.head(limit)

        return comments_df

    def get_columns(self) -> List[str]:
        """Get the list of column names for the comments table.
        Returns:
            list: A list of column names for the comments table.
        """
        return [
            'id',
            'by',
            'parent',
            'text',
            'time',
            'type',
        ]

    def filter_columns(self, result: pd.DataFrame, query: ast.Select = None) -> None:
        """Filter the columns of a DataFrame to those specified in an SQL query.
        Args:
            result (pandas.DataFrame): The DataFrame to filter.
            query (ast.Select): The SQL query containing the column names to filter on.
        """
        if query is None:
            return

        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                return
            elif isinstance(target, ast.Identifier):
                columns.append(target.value)

        if len(columns) > 0:
            result = result[columns]
