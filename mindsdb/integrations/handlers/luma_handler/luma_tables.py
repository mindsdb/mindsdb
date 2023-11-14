import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities.log import get_log
from mindsdb_sql.parser import ast

logger = get_log("integrations.luma_handler")


class LumaListEventsTable(APITable):
    """The Luma List Events Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.lu.ma/reference/calendar-list-events" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Luma events

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'list_events',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.luma_client.list_events()

        content = response["content"]["entries"]

        df = pd.json_normalize(content)

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "api_id",
            "event.api_id",
            "event.cover_url",
            "event.name",
            "event.description",
            "event.series_api_id",
            "event.start_at",
            "event.duration_interval",
            "event.end_at",
            "event.geo_address_json.city",
            "event.geo_address_json.type",
            "event.geo_address_json.region",
            "event.geo_address_json.address",
            "event.geo_address_json.country",
            "event.geo_address_json.latitude",
            "event.geo_address_json.place_id",
            "event.geo_address_json.longitude",
            "event.geo_address_json.city_state",
            "event.geo_address_json.description",
            "event.geo_address_json.full_address",
            "event.geo_latitude",
            "event.geo_longitude",
            "event.url",
            "event.timezone",
            "event.event_type",
            "event.user_api_id",
            "event.visibility"
        ]


class LumaGetEventTable(APITable):
    """The Luma Get Event Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.lu.ma/reference/get-event-1" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Luma events

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_event',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'event_id':
                if op == '=':
                    search_params["event_id"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for event_id column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("event_id" in search_params)

        if not filter_flag:
            raise NotImplementedError("event_id column has to be present in WHERE clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.luma_client.get_event(search_params["event_id"])

        content = response["content"]

        df = pd.json_normalize(content)

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "hosts",
            "event.api_id",
            "event.cover_url",
            "event.name",
            "event.description",
            "event.series_api_id",
            "event.start_at",
            "event.duration_interval",
            "event.end_at",
            "event.geo_address_json.city",
            "event.geo_address_json.type",
            "event.geo_address_json.region",
            "event.geo_address_json.address",
            "event.geo_address_json.country",
            "event.geo_address_json.latitude",
            "event.geo_address_json.place_id",
            "event.geo_address_json.longitude",
            "event.geo_address_json.city_state",
            "event.geo_address_json.description",
            "event.geo_address_json.full_address",
            "event.geo_latitude",
            "event.geo_longitude",
            "event.url",
            "event.timezone",
            "event.event_type",
            "event.user_api_id",
            "event.visibility"
        ]
