import pandas as pd
from mindsdb_sql_parser import ast
from pandas import DataFrame

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.date_utils import parse_utc_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class SearchAnalyticsTable(APITable):
    """
    Table class for the Google Search Console Search Analytics table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all traffic data from the Search Console.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        accepted_params = ['siteUrl', 'dimensions', 'type', 'rowLimit', 'aggregationType']
        for op, arg1, arg2 in conditions:
            if arg1 == 'startDate' or arg1 == 'endDate':
                date = parse_utc_date(arg2)
                if op == '=':
                    params[arg1] = date
                else:
                    raise NotImplementedError
            elif arg1 in accepted_params:
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
            else:
                raise NotImplementedError

        dimensions = ['query', 'page', 'device', 'country']

        # Get the group by from the query.
        params['dimensions'] = {}
        conditions = extract_comparison_conditions(query.group_by)
        for arg1 in conditions:
            if arg1 in dimensions:
                params['dimensions'][arg1] = arg1
            else:
                raise NotImplementedError

        # Get the order by from the query.
        if query.order_by is not None:
            if query.order_by[0].value == 'start_time':
                params['orderBy'] = 'startTime'
            elif query.order_by[0].value == 'updated':
                params['orderBy'] = 'updated'
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['rowLimit'] = query.limit.value

        # Get the traffic data from the Google Search Console API.
        traffic_data = self.handler. \
            call_application_api(method_name='get_traffic_data', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(traffic_data) == 0:
            traffic_data = pd.DataFrame([], columns=selected_columns)
        else:
            traffic_data.columns = self.get_columns()
            for col in set(traffic_data.columns).difference(set(selected_columns)):
                traffic_data = traffic_data.drop(col, axis=1)
        return traffic_data

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'keys',
            'clicks',
            'impressions',
            'ctr',
            'position'
        ]


class SiteMapsTable(APITable):
    """
    Table class for the Google Search Console Site Maps table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all traffic data from the Search Console.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        accepted_params = ['siteUrl', 'sitemapIndex']
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError
            if arg1 in accepted_params:
                params[arg1] = arg2
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['rowLimit'] = query.limit.value

        # Get the traffic data from the Google Search Console API.
        sitemaps = self.handler. \
            call_application_api(method_name='get_sitemaps', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(sitemaps) == 0:
            sitemaps = pd.DataFrame([], columns=selected_columns)
        else:
            sitemaps.columns = self.get_columns()
            for col in set(sitemaps.columns).difference(set(selected_columns)):
                sitemaps = sitemaps.drop(col, axis=1)
        return sitemaps

    def insert(self, query: ast.Insert):
        """
        Submits a sitemap for a site.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Get the values from the query.
        values = query.values[0]
        params = {}
        # Get the event data from the values.
        for col, val in zip(query.columns, values):
            if col == 'siteUrl' or col == 'feedpath':
                params[col] = val
            else:
                raise NotImplementedError

        # Insert the event into the Google Calendar API.
        self.handler.call_application_api(method_name='submit_sitemap', params=params)

    def delete(self, query: ast.Delete):
        """
        Deletes a sitemap for a site.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError
            if arg1 == 'siteUrl' or arg1 == 'feedpath':
                params[arg1] = arg2
            else:
                raise NotImplementedError

        # Delete the events in the Google Calendar API.
        self.handler.call_application_api(method_name='delete_sitemap', params=params)

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'path',
            'lastSubmitted',
            'isPending',
            'isSitemapsIndex',
            'type',
            'lastDownloaded',
            'warnings',
            'errors',
            'contents'
        ]
