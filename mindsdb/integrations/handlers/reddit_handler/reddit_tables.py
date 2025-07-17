import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class CommentTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        '''Select data from the comment table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        '''

        reddit = self.handler.connect()

        submission_id = None
        conditions = extract_comparison_conditions(query.where)
        for condition in conditions:
            if condition[0] == '=' and condition[1] == 'submission_id':
                submission_id = condition[2]
                break

        if submission_id is None:
            raise ValueError('Submission ID is missing in the SQL query')

        submission = reddit.submission(id=submission_id)
        submission.comments.replace_more(limit=None)

        result = []
        for comment in submission.comments.list():
            data = {
                'id': comment.id,
                'body': comment.body,
                'author': comment.author.name if comment.author else None,
                'created_utc': comment.created_utc,
                'score': comment.score,
                'permalink': comment.permalink,
                'ups': comment.ups,
                'downs': comment.downs,
                'subreddit': comment.subreddit.display_name,
            }
            result.append(data)

        result = pd.DataFrame(result)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        '''Get the list of column names for the comment table.

        Returns:
            list: A list of column names for the comment table.
        '''
        return [
            'id',
            'body',
            'author',
            'created_utc',
            'permalink',
            'score',
            'ups',
            'downs',
            'subreddit',
        ]

    def filter_columns(self, result: pd.DataFrame, query: ast.Select = None):
        columns = []
        if query is not None:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    columns = self.get_columns()
                    break
                elif isinstance(target, ast.Identifier):
                    columns.append(target.value)
        if len(columns) > 0:
            result = result[columns]


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
        sort_type = None
        conditions = extract_comparison_conditions(query.where)
        for condition in conditions:
            if condition[0] == '=' and condition[1] == 'subreddit':
                subreddit_name = condition[2]
            elif condition[0] == '=' and condition[1] == 'sort_type':
                sort_type = condition[2]
            elif condition[0] == '=' and condition[1] == 'items':
                items = int(condition[2])

        if not sort_type:
            sort_type = 'hot'
        if not subreddit_name:
            return pd.DataFrame()

        if sort_type == 'new':
            submissions = reddit.subreddit(subreddit_name).new(limit=items)
        elif sort_type == 'rising':
            submissions = reddit.subreddit(subreddit_name).rising(limit=items)
        elif sort_type == 'controversial':
            submissions = reddit.subreddit(subreddit_name).controversial(limit=items)
        elif sort_type == 'top':
            submissions = reddit.subreddit(subreddit_name).top(limit=items)
        else:
            submissions = reddit.subreddit(subreddit_name).hot(limit=items)

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
