from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser import ast
import datetime
import pytz
import time
from tzlocal import get_localzone

class GoogleFitTable(APITable):

    def time_parser(args) -> int:
        ymd = args.split('-')
        epoch0 = datetime(1970, 1, 1, tzinfo=pytz.utc)
        time = pytz.timezone(str(get_localzone())).localize(datetime(int(ymd[0].rstrip()), int(ymd[1].rstrip()), int(ymd[2].rstrip())))
        return int((time - epoch0).total_seconds() * 1000)
    
    def select(self, query: ast.Select) -> Response:

        conditions = extract_comparison_conditions(query.where)
        
        params = {}
        filters = []
        steps = {}
        for op, arg1, arg2 in conditions:
            if op == 'or':
                raise NotImplementedError(f'OR is not supported')
            if arg1 == 'date':
                date = self.time_parser(arg2)
                if op == '>':
                    params['start_time'] = date
                    params['end_time'] = int(round(time.time() * 1000))
                elif op == '<':
                    params['start_time'] = date - 31536000000
                    params['end_time'] = date
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError(f'This query is not supported')

        result = self.handler.call_google_fit_api(
            method_name='get_steps',
            params=params
        )
        return result

    def get_columns(self):
        return [
            'id',
            'created_at',
            'text',
            'edit_history_tweet_ids',
            'author_id',
            'author_name',
            'author_username',
            'conversation_id',
            'in_reply_to_tweet_id',
            'in_retweeted_to_tweet_id',
            'in_quote_to_tweet_id',
            'in_reply_to_user_id',
            'in_reply_to_user_name',
            'in_reply_to_user_username',
        ]     