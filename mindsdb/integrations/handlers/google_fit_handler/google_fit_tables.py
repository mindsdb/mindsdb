from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser import ast
import datetime
import pytz
import time
from tzlocal import get_localzone


class GoogleFitTable(APITable):

    def time_parser(self, args) -> int:
        """
        Receive raw date string and return the calculated milliseconds based on the time string.
        Args:
            args: time string in the format of YYYY-MM-DD
        Returns:
            the input time string in the format of milliseconds
        """
        ymd = args.split('-')
        epoch0 = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
        time = pytz.timezone(str(get_localzone())).localize(datetime.datetime(int(ymd[0].rstrip()), int(ymd[1].rstrip()), int(ymd[2].rstrip())))
        return int((time - epoch0).total_seconds() * 1000)

    def select(self, query: ast.Select) -> Response:

        conditions = extract_comparison_conditions(query.where)

        params = {}
        # get the local time
        now = int(round(time.time() * 1000))

        # hard coded for now as user default query time period
        one_month = 2629746000
        for op, arg1, arg2 in conditions:
            if op == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'date':
                date = self.time_parser(arg2)
                if op == '>':
                    params['start_time'] = date
                    params['end_time'] = now

                # hard coded as a month
                elif op == '<':
                    params['start_time'] = date - one_month
                    params['end_time'] = date
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError('This query is not supported')
        # if time is not provided in the query, the time range is one month ago to now
        if not params:
            params['start_time'] = now - one_month
            params['end_time'] = now
        result = self.handler.call_google_fit_api(
            method_name='get_steps',
            params=params
        )
        return result

    def get_columns(self):
        return [
            'dates',
            'steps'
        ]
