import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities.log import get_log
from mindsdb_sql.parser import ast

logger = get_log("integrations.luma_handler")


class LumaEventsTable(APITable):
    """The Luma Get Event Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.lu.ma/reference/get-event-1 and https://docs.lu.ma/reference/calendar-list-events" API

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
            'events',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        filter_flag = False
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'event_id':
                if op == '=':
                    search_params["event_id"] = arg2
                    filter_flag = True
                else:
                    raise NotImplementedError("Only '=' operator is supported for event_id column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        df = pd.DataFrame(columns=self.get_columns())

        if filter_flag:
            response = self.handler.luma_client.get_event(search_params["event_id"])
            event = response["content"]["event"]
            df = pd.json_normalize(event)
        else:
            response = self.handler.luma_client.list_events()
            content = response["content"]["entries"]
            events_only = [event["event"] for event in content]
            df = pd.json_normalize(events_only)

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def get_columns(self) -> list:
        return ["api_id",
                "cover_url",
                "name",
                "description",
                "series_api_id",
                "start_at",
                "duration_interval",
                "end_at",
                "geo_latitude",
                "geo_longitude",
                "url",
                "timezone",
                "event_type",
                "user_api_id",
                "visibility",
                "geo_address_json.city",
                "geo_address_json.type",
                "geo_address_json.region",
                "geo_address_json.address",
                "geo_address_json.country",
                "geo_address_json.latitude",
                "geo_address_json.place_id",
                "geo_address_json.longitude",
                "geo_address_json.city_state",
                "geo_address_json.description",
                "geo_address_json.full_address"]
