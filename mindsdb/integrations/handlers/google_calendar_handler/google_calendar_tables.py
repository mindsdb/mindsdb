import pandas as pd
import datetime
from mindsdb_sql_parser import ast
from pandas import DataFrame

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.date_utils import utc_date_str_to_timestamp_ms, parse_utc_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class GoogleCalendarEventsTable(APITable):
    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all events from the calendar.

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
            if arg1 == "timeMax" or arg1 == "timeMin":
                date = parse_utc_date(arg2)
                if op == "=":
                    params[arg1] = date
                else:
                    raise NotImplementedError
            elif arg1 == "timeZone":
                params[arg1] = arg2
            elif arg1 == "maxAttendees":
                params[arg1] = arg2
            elif arg1 == "q":
                params[arg1] = arg2

        # Get the order by from the query.
        if query.order_by is not None:
            if query.order_by[0].value == "start_time":
                params["orderBy"] = "startTime"
            elif query.order_by[0].value == "updated":
                params["orderBy"] = "updated"
            else:
                raise NotImplementedError

        if query.limit is not None:
            params["maxResults"] = query.limit.value

        # Get the events from the Google Calendar API.
        events = self.handler.call_application_api(method_name="get_events", params=params)

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
        Inserts an event into the calendar.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Get the values from the query.
        values = query.values[0]
        # Get the event data from the values.
        event_data = {}
        timestamp_columns = {"start_time", "end_time", "created", "updated"}
        regular_columns = {
            "summary",
            "description",
            "location",
            "status",
            "html_link",
            "creator",
            "organizer",
            "reminders",
            "timeZone",
            "calendar_id",
            "attendees",
        }

        # TODO: check why query.columns is None
        for col, val in zip(query.columns, values):
            if col.name in timestamp_columns:
                event_data[col.name] = utc_date_str_to_timestamp_ms(val)
            elif col.name in regular_columns:
                event_data[col.name] = val
            else:
                raise NotImplementedError

        st = datetime.datetime.fromtimestamp(event_data["start_time"] / 1000, datetime.timezone.utc).isoformat() + "Z"
        et = datetime.datetime.fromtimestamp(event_data["end_time"] / 1000, datetime.timezone.utc).isoformat() + "Z"

        event_data["start"] = {"dateTime": st, "timeZone": event_data["timeZone"]}

        event_data["end"] = {"dateTime": et, "timeZone": event_data["timeZone"]}

        event_data["attendees"] = event_data["attendees"].split(",")
        event_data["attendees"] = [{"email": attendee} for attendee in event_data["attendees"]]

        # Insert the event into the Google Calendar API.
        self.handler.call_application_api(method_name="create_event", params=event_data)

    def update(self, query: ast.Update):
        """
        Updates an event or events in the calendar.

        Args:
            query (ast.Update): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Get the values from the query.
        values = query.values[0]
        # Get the event data from the values.
        event_data = {}
        for col, val in zip(query.update_columns, values):
            if col == "start_time" or col == "end_time" or col == "created" or col == "updated":
                event_data[col] = utc_date_str_to_timestamp_ms(val)
            elif (
                col == "summary"
                or col == "description"
                or col == "location"
                or col == "status"
                or col == "html_link"
                or col == "creator"
                or col == "organizer"
                or col == "reminders"
                or col == "timeZone"
                or col == "calendar_id"
                or col == "attendees"
            ):
                event_data[col] = val
            else:
                raise NotImplementedError

        event_data["start"] = {"dateTime": event_data["start_time"], "timeZone": event_data["timeZone"]}

        event_data["end"] = {"dateTime": event_data["end_time"], "timeZone": event_data["timeZone"]}

        event_data["attendees"] = event_data.get("attendees").split(",")
        event_data["attendees"] = [{"email": attendee} for attendee in event_data["attendees"]]

        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if arg1 == "event_id":
                if op == "=":
                    event_data["event_id"] = arg2
                elif op == ">":
                    event_data["start_id"] = arg2
                elif op == "<":
                    event_data["end_id"] = arg2
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError

        # Update the event in the Google Calendar API.
        self.handler.call_application_api(method_name="update_event", params=event_data)

    def delete(self, query: ast.Delete):
        """
        Deletes an event or events in the calendar.

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
            if arg1 == "event_id":
                if op == "=":
                    params[arg1] = arg2
                elif op == ">":
                    params["start_id"] = arg2
                elif op == "<":
                    params["end_id"] = arg2
                else:
                    raise NotImplementedError

        # Delete the events in the Google Calendar API.
        self.handler.call_application_api(method_name="delete_event", params=params)

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            "etag",
            "id",
            "status",
            "htmlLink",
            "created",
            "updated",
            "summary",
            "creator",
            "organizer",
            "start",
            "end",
            "timeZone",
            "iCalUID",
            "sequence",
            "reminders",
            "eventType",
        ]
