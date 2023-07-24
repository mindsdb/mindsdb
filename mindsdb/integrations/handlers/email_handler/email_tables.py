import datetime as dt
import ast
import pytz

import pandas as pd

from mindsdb_sql.parser import ast
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response
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
            mailbox, subject, to, from_, since_emailid = None
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

    def insert(self, query: ast.Insert):
        # https://docs.tweepy.org/en/stable/client.html#tweepy.Client.create_tweet
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))
            self.handler.('create_tweet', params)