import pandas as pd
from typing import List
import re

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class ConversionEventsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Gets all conversion events from google analytics property.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """
        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'page_size':
                params[arg1] = arg2
            else:
                raise NotImplementedError

        # Get the order by from the query.
        if query.order_by is not None:
            raise NotImplementedError

        if query.limit is not None:
            raise NotImplementedError

        # Get the conversion events from the Google Analytics Admin API.
        conversion_events = pd.DataFrame(columns=self.get_columns())
        result = self.handler.get_conversion_events(params=params)
        conversion_events_data = self.extract_conversion_events_data(result.conversion_events)
        events = self.concat_dataframes(conversion_events, conversion_events_data)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(events) == 0:
            events = pd.DataFrame([], columns=selected_columns)
        else:
            events.columns = self.get_columns()
            for col in set(events.columns).difference(set(selected_columns)):
                events = events.drop(col, axis=1)
        return events

    def insert(self, query: ast.Insert):
        """
        Inserts a conversion event into your GA4 property.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """
        columns = [col.name for col in query.columns]

        supported_columns = {'event_name', 'countingMethod'}
        if not set(columns).issubset(supported_columns):
            unsupported_columns = set(columns).difference(supported_columns)
            raise ValueError(
                "Unsupported columns for create conversion event: "
                + ", ".join(unsupported_columns)
            )
        params = {}

        for row in query.values:
            params = dict(zip(columns, row))

        if params['countingMethod'] == 1 or params['countingMethod'] == 0:
            params['countingMethod'] = 1
        else:
            params['countingMethod'] = 2

        # Insert the conversion event into the Google Analytics Admin API.
        conversion_events = pd.DataFrame(columns=self.get_columns())
        result = self.handler.create_conversion_event(params=params)
        conversion_events_data = self.extract_conversion_events_data([result])
        self.concat_dataframes(conversion_events, conversion_events_data)

    def update(self, query: ast.Update):
        """
        Updates a conversion event into your GA4 property.

        Args:
            query (ast.Update): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """
        # Get the values from the query.
        values = query.update_columns.items()
        data_list = list(values)
        # Get the conversion event data from the values.
        params = {}
        for col, val in zip(query.update_columns, data_list):
            params[col] = val

        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if arg1 == 'name':
                if op == '=':
                    params['name'] = arg2
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError

        counting_method_value = str(params['countingMethod'][1])

        # Use regex to check if the expression contains 1 or 0
        if re.search(r'[01]', counting_method_value):
            params['countingMethod'] = 1
        else:
            params['countingMethod'] = 2

        # Update the conversion event in the Google Analytics Admin API.
        conversion_events = pd.DataFrame(columns=self.get_columns())
        result = self.handler.update_conversion_event(params=params)
        conversion_events_data = self.extract_conversion_events_data([result])
        self.concat_dataframes(conversion_events, conversion_events_data)

    def delete(self, query: ast.Delete):
        """
        Deletes a conversion event into your GA4 property.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if op == 'or':
                raise NotImplementedError('OR is not supported')
            if arg1 == 'name':
                if op == '=':
                    self.handler.delete_conversion_event(params={'name': arg2})
                else:
                    raise NotImplementedError(f'Unknown op: {op}')
            else:
                raise NotImplementedError(f'Unknown clause: {arg1}')

    @staticmethod
    def extract_conversion_events_data(conversion_events):
        """
        Extract conversion events data and return a list of lists.
        Args:
            conversion_events: List of ConversionEvent objects
        Returns:
            List of lists containing conversion event data
        """
        conversion_events_data = []
        for conversion_event in conversion_events:
            data_row = [
                conversion_event.name,
                conversion_event.event_name,
                conversion_event.create_time,
                conversion_event.deletable,
                conversion_event.custom,
                conversion_event.ConversionCountingMethod(conversion_event.counting_method).name,
            ]
            conversion_events_data.append(data_row)
        return conversion_events_data

    def concat_dataframes(self, existing_df, data):
        """
        Concatenate existing DataFrame with new data.
        Args:
            existing_df: Existing DataFrame
            data: New data to be added to the DataFrame
        Returns:
            Concatenated DataFrame
        """
        return pd.concat(
            [existing_df, pd.DataFrame(data, columns=self.get_columns())],
            ignore_index=True
        )

    def get_columns(self) -> List[str]:
        """
        Gets all columns to be returned in pandas DataFrame responses

        Returns:
        List[str]: List of columns
        """
        return [
            'name',
            'event_name',
            'create_time',
            'deletable',
            'custom',
            'countingMethod',
        ]
