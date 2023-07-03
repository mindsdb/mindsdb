import datetime as dt
import email_helpers
import ast
from collections import defaultdict
import pytz

import pandas as pd

from mindsdb.utilities import log

from mindsdb_sql.parser import ast
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


def extract_conditions(binary_op):
    conditions = []

    def _extract_conditions(node, **kwargs):
        if isinstance(node, ast.BinaryOperation):
            op = node.op.lower()
            if op == 'and':
                return
            elif op == 'or':
                raise NotImplementedError
            elif not isinstance(node.args[0], ast.Identifier) or not isinstance(node.args[1], ast.Constant):
                raise NotImplementedError
            conditions.append([op, node.args[0].parts[-1], node.args[1].value])

    query_traversal(binary_op, _extract_conditions)
    return conditions


def parse_date(date_str):
    if isinstance(date_str, dt.datetime):
        return date_str
    date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
    date = None
    for date_format in date_formats:
        try:
            date = dt.datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    if date is None:
        raise ValueError(f"Can't parse date: {date_str}")
    date = date.astimezone(pytz.utc)
    return date


class EmailsTable(APITable):
    
    def select(self, query: ast.Select) -> Response:

        conditions = extract_conditions(query.where)

        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'created_at':
                date = parse_date(arg2)
                if op == '>':
                    params['since_date'] = date
                elif op == '<':
                    params['until_date'] = date
                else:
                    raise NotImplementedError
                continue
            elif arg1 == 'id':
                
                if op == '>':
                    params['since_emailid'] = arg2
                else:
                    raise NotImplementedError
                continue
            if op == '=' and arg1 in ['mailbox', 'subject', 'to', 'from']:
                raise NotImplementedError
            mailbox, subject, to, from_,  since_emailid=None
            params[arg1] = arg2

        if query.limit is not None:
            params['max_results'] = query.limit.value

        params['expansions'] = ['author_id', 'in_reply_to_user_id']
        params['tweet_fields'] = ['created_at']
        params['user_fields'] = ['name', 'username']

        if 'query' not in params:
            # search not works without query, use 'mindsdb'
            params['query'] = 'mindsdb'

        result = self.handler.call_twitter_api(
            method_name='search_recent_tweets',
            params=params
        )

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()
        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None
        return result

    def get_columns(self):
        return ['id', 'created_at', 'to', 'from', 'subject', 'body']

    def insert(self, query:ast.Insert):
        # https://docs.tweepy.org/en/stable/client.html#tweepy.Client.create_tweet
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))
            self.handler.('create_tweet', params)


class EmailHandler(APIHandler):
    """A class for handling connections and interactions with Email (send and search).

    Attributes:
        "email": "Email address used to login to the SMTP and IMAP servers.",
        "password": "Password used to login to the SMTP and IMAP servers.",
        "smtp_server": "SMTP server to be used for sending emails. Default value is 'smtp.gmail.com'.",
        "smtp_port": "Port number for the SMTP server. Default value is 587.",
        "imap_server": "IMAP server to be used for reading emails. Default value is 'imap.gmail.com'."
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        for k in ['email', 'smtp_server', 'smtp_port', 'imap_server',
                  'username', 'password']:
            if k in args:
                self.connection_args[k] = args[k]

        self.api = None
        self.is_connected = False

        emails = EmailsTable(self)
        self._register_table('emails', emails)

    def connect(self):
        """Authenticate with the email servers using credentials."""

        if self.is_connected is True:
            return self.api

        try:
            self.api = email_helpers.EmailClient(**self.connection_args)
        except Exception as e:
            log.logger.error(f'Error connecting to email api: {e}!')
            raise e
        
        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            api = self.connect()
            api.get_user(id=1)

            response.success = True

        except Exception as e:
            log.logger.error(f'Error connecting to email api: {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_email_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_email_api(self, method_name:str = None, params:dict = None):

        # method > table > columns
        expansions_map = {
            'search_recent_tweets': {
                'users': ['author_id', 'in_reply_to_user_id'],
            },
            'search_all_tweets': {
                'users': ['author_id'],
            },
        }

        self.connect()
        method = getattr(self.api.imap_server, method_name)

        # pagination handle

        includes = defaultdict(list)

        resp, items = resp = method(**params)
        
        df = pd.DataFrame(items)

        return df

