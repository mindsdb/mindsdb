import praw
import pandas as pd
from datetime import datetime
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.config import Config
from mindsdb.utilities import log
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb_sql.parser.ast import Select
class RedditTable(APITable):
    '''A class representing a table for Reddit API data.

    This class inherits from APITable and provides functionality to select data
    from the Reddit API and return it as a pandas DataFrame.

    Methods:
        select(ast.Select): Select data from the Reddit table and return it as a pandas DataFrame.
        get_columns(): Get the list of column names for the Reddit table.

    '''

    def select(self, query: ast.Select) :
        '''Select data from the Reddit table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        '''
        # Extract the subreddit name from the query
        subreddit = query.from_table.table_name

        # Initialize the Reddit handler
        handler = self.handler

        # Connect to the Reddit API
        reddit = praw.Reddit(
            client_id=handler.client_id,
            client_secret=handler.client_secret,
            user_agent=handler.user_agent
        )

        # Create an empty list to store the data
        data = []

        # Retrieve the submissions from the subreddit
        for submission in reddit.subreddit(subreddit).new(limit=query.limit.value if query.limit else None):
            # Extract the data from the submission
            record = {
                'id': submission.id,
                'created_utc': datetime.fromtimestamp(submission.created_utc),
                'author': submission.author.name if submission.author else None,
                'title': submission.title,
                'body': submission.selftext,
                'subreddit': submission.subreddit.display_name,
            }
            # Append the data to the list
            data.append(record)

        # Create a DataFrame from the data
        df = pd.DataFrame(data)

        # Filter the DataFrame based on the query conditions
        if query.where is not None:
            for condition in query.where.conditions:
                column = condition.left.value
                operator = condition.operator.value
                value = condition.right.value
                if operator == '=':
                    df = df[df[column] == value]
                elif operator == '>':
                    df = df[df[column] > value]
                elif operator == '>=':
                    df = df[df[column] >= value]
                elif operator == '<':
                    df = df[df[column] < value]
                elif operator == '<=':
                    df = df[df[column] <= value]
                elif operator == 'LIKE':
                    df = df[df[column].str.contains(value)]

        # Filter the DataFrame based on the query columns
        if isinstance(query.targets[0], ast.Identifier):
            columns = [target.value for target in query.targets]
            df = df[columns]

        return df

    def get_columns(self):
        '''Get the list of column names for the Reddit table.

        Returns:
            list: A list of column names for the Reddit table.
        '''
        return ['id', 'created_utc', 'author', 'title', 'body', 'subreddit']



class SubredditTable(APITable):
    '''A class representing the subreddit table.

    This class inherits from APITable and provides functionality to select data
    from the subreddit endpoint of the Reddit API and return it as a pandas DataFrame.

    Methods:
        select(ast.Select): Select data from the subreddit table and return it as a pandas DataFrame.
        get_columns(): Get the list of column names for the subreddit table.

    '''

    def select(self, query: ast.Select):
        '''Select data from the subreddit table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        '''

        reddit = self.handler.connect()

        where_clause = query.where
        if where_clause is not None:
            # Parse the where clause to extract the comparison conditions
            condition = where_clause.conditions[0]
            column_name = condition.left.parts[-1]
            column_value = condition.right.value
            operator = condition.operator

            # Filter the subreddits based on the comparison condition
            if operator == '=':
                subreddits = reddit.subreddit(column_value)
            else:
                raise Exception('Only equals to "=" is supported')

            result = []
            for subreddit in subreddits:
                data = {
                    'subreddit_id': subreddit.id,
                    'name': subreddit.display_name,
                    'title': subreddit.title,
                    'description': subreddit.public_description,
                    'url': subreddit.url,
                    'subscribers': subreddit.subscribers,
                    'created_utc': subreddit.created_utc,
                }
                result.append(data)

        else:
            # If there is no where clause, select all subreddits
            subreddits = reddit.subreddits()
            result = []
            for subreddit in subreddits:
                data = {
                    'subreddit_id': subreddit.id,
                    'name': subreddit.display_name,
                    'title': subreddit.title,
                    'description': subreddit.public_description,
                    'url': subreddit.url,
                    'subscribers': subreddit.subscribers,
                    'created_utc': subreddit.created_utc,
                }
                result.append(data)

        result = pd.DataFrame(result)
        self.filter_columns(query=query, result=result)
        return result

    def get_columns(self):
        '''Get the list of column names for the subreddit table.

        Returns:
            list: A list of column names for the subreddit table.
        '''

        return [
            'subreddit_id',
            'name',
            'title',
            'description',
            'url',
            'subscribers',
            'created_utc',
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

class SubmissionTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        '''Select data from the submission table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        '''

        reddit = self.handler.connect()

        subreddit_name = None
        conditions = extract_comparison_conditions(query.where)
        for condition in conditions:
            if condition[0] == '=' and condition[1] == 'subreddit':
                subreddit_name = condition[2]
                break

        if subreddit_name is None:
            raise ValueError('Subreddit name is missing in the SQL query')

        submissions = reddit.subreddit(subreddit_name).hot(limit=100)

        result = []
        for submission in submissions:
            data = {
                'id': submission.id,
                'title': submission.title,
                'author': submission.author.name if submission.author else None,
                'created_utc': submission.created_utc,
                'score': submission.score,
                'num_comments': submission.num_comments,
                'permalink': submission.permalink,
                'url': submission.url,
                'ups': submission.ups,
                'downs': submission.downs,
                'num_crossposts': submission.num_crossposts,
                'subreddit': submission.subreddit.display_name,
                'selftext': submission.selftext,
            }
            result.append(data)

        result = pd.DataFrame(result)
        self.filter_columns(result, query)
        return result


    def get_columns(self):
        '''Get the list of column names for the submission table.

        Returns:
            list: A list of column names for the submission table.
        '''
        return [
            'id',
            'title',
            'author',
            'created_utc',
            'permalink',
            'url',
            'num_comments',
            'score',
            'ups',
            'downs',
            'num_crossposts',
            'subreddit',
            'selftext'
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

class RedditHandler(APIHandler):


    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = Config().get('reddit_handler', {})
        for k in ['client_id', 'client_secret', 'user_agent']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'REDDIT_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'REDDIT_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.reddit = None
        self.is_connected = False

        subreddit = SubredditTable(self)
        self._register_table('subreddit', subreddit)

        submission = SubmissionTable(self)
        self._register_table('submission', submission)

    def connect(self):
        """Authenticate with the Reddit API using the client ID, client secret, username, password, and user agent provided in the constructor."""
        if self.is_connected is True:
            return self.reddit

        self.reddit = praw.Reddit(
            client_id=self.connection_args['client_id'],
            client_secret=self.connection_args['client_secret'],
            user_agent=self.connection_args['user_agent'],
        )

        self.is_connected = True
        return self.reddit

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            reddit = self.connect()

            # Try to get the front page of reddit
            reddit.front

            response.success = True

        except praw.exceptions.PRAWException as e:
            response.error_message = f'Error connecting to Reddit api: {e}'
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response


    def check_connection(self) -> StatusResponse:
        '''It evaluates if the connection with Reddit API is alive and healthy.
        Returns:
            HandlerStatusResponse
        '''

        response = StatusResponse(False)

        try:
            reddit = self.connect()
            reddit.user.me()
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Reddit api: {e}. '
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        '''It parses any native statement string and acts upon it (for example, raw syntax commands).
        Args:
        query (Any): query in native format (str for sql databases,
            dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        '''

        method_name, params = self.parse_native_query(query_string)
        if method_name == 'get_submission':
            df = self.get_submission(params)
        elif method_name == 'get_subreddit':
            df = self.get_subreddit(params)
        else:
            raise ValueError(f"Method '{method_name}' not supported by RedditHandler")

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def parse_native_query(self, query_string: str):
        query_parts = query_string.split('.')
        method_name = query_parts[-1]
        if method_name not in ['get_submission', 'get_subreddit']:
            raise ValueError(f"Method '{method_name}' not supported by RedditHandler")
        params_str = query_parts[-2].split('(')[-1].split(')')[0]
        params = {}
        if params_str:
            for p in params_str.split(','):
                key, value = p.split('=')
                params[key.strip()] = value.strip()

