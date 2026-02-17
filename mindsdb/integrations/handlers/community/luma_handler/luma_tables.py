import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor, INSERTQueryParser
from mindsdb_sql_parser import ast


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

    def _parse_event_insert_data(self, event):
        data = {}
        data["name"] = event["name"]
        data["start_at"] = event["start_at"]
        data["timezone"] = event["timezone"]

        if "end_at" in event:
            data["end_at"] = event["end_at"]

        if "require_rsvp_approval" in event:
            data["require_rsvp_approval"] = event["require_rsvp_approval"]

        if "geo_latitude" in event:
            data["geo_latitude"] = event["geo_latitude"]

        if "geo_longitude" in event:
            data["geo_longitude"] = event["geo_longitude"]

        if "meeting_url" in event:
            data["meeting_url"] = event["meeting_url"]

        data["geo_address_json"] = {}

        if "geo_address_json_type" in event:
            data["geo_address_json"]["type"] = event["geo_address_json_type"]

        if "geo_address_json_place_id" in event:
            data["geo_address_json"]["place_id"] = event["geo_address_json_place_id"]

        if "geo_address_json_description" in event:
            data["geo_address_json"]["description"] = event["geo_address_json_description"]

        return data

    def insert(self, query: ast.ASTNode) -> None:
        """Inserts data into the API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=["name", "start_at", "timezone", "end_at", "require_rsvp_approval", "geo_address_json_type", "geo_address_json_place_id", "geo_address_json_description", "geo_latitude", "geo_longitude", "meeting_url"],
            mandatory_columns=["name", "start_at", "timezone"],
            all_mandatory=False
        )

        event_data = insert_statement_parser.parse_query()

        for event in event_data:
            parsed_event_data = self._parse_event_insert_data(event)
            self.handler.luma_client.create_event(parsed_event_data)
