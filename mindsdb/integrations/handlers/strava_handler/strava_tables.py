import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

from mindsdb_sql_parser import ast


logger = log.getLogger(__name__)


class StravaAllClubsTable(APITable):
    """Strava List all Clubs Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Strava  "getLoggedInAthleteClubs" API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            strava "List Athlete Clubs " matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "id":
                    next
                if an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )
        strava_clubs_df = self.call_strava_allclubs_api()

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(strava_clubs_df) == 0:
            strava_clubs_df = pd.DataFrame([], columns=selected_columns)
        else:
            strava_clubs_df.columns = self.get_columns()
            for col in set(strava_clubs_df.columns).difference(set(selected_columns)):
                strava_clubs_df = strava_clubs_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                strava_clubs_df = strava_clubs_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        if query.limit:
            strava_clubs_df = strava_clubs_df.head(query.limit.value)

        return strava_clubs_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
            'id',
            'name',
            'sport_type',
            'city',
            'state',
            'country',
            'member_count',
        ]

    def call_strava_allclubs_api(self):
        """Pulls all the records from the given and returns it select()

        Returns
        -------
        pd.DataFrame of all the records of the "List Athlete Clubs" API end point
        """

        clubs = self.handler.connect().get_athlete_clubs()

        club_cols = self.get_columns()
        data = []

        for club in clubs:
            club_dict = club.dict()
            data.append([club_dict.get(x) for x in club_cols])

        all_strava_clubs_df = pd.DataFrame(data, columns=club_cols)

        return all_strava_clubs_df


class StravaClubActivitesTable(APITable):
    """Strava List Club Activities by id Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Strava  "List Club Activities " API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            strava "List Club Activities" matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        conditions = extract_comparison_conditions(query.where)

        order_by_conditions = {}
        clubs_kwargs = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "id":
                    next
                if an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "strava_club_id":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for strava_club_id ")
                clubs_kwargs["type"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        strava_club_activities_df = self.call_strava_clubactivities_api(a_where[2])

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(strava_club_activities_df) == 0:
            strava_club_activities_df = pd.DataFrame([], columns=selected_columns)
        else:
            strava_club_activities_df.columns = self.get_columns()
            for col in set(strava_club_activities_df.columns).difference(set(selected_columns)):
                strava_club_activities_df = strava_club_activities_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                strava_club_activities_df = strava_club_activities_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        if query.limit:
            strava_club_activities_df = strava_club_activities_df.head(query.limit.value)

        return strava_club_activities_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
            'name',
            'distance',
            'moving_time',
            'elapsed_time',
            'total_elevation_gain',
            'sport_type',
            'athlete.firstname',
        ]

    def call_strava_clubactivities_api(self, club_id):
        """Pulls all the records from the given and returns it select()

        Returns
        -------
        pd.DataFrame of all the records of the "getClubActivitiesById" API end point
        """

        club_activities = self.handler.connect().get_club_activities(club_id)

        club_cols = self.get_columns()
        data = []

        for club in club_activities:
            club_dict = club.dict()
            data.append([club_dict.get(x) for x in club_cols])

        all_strava_club_activities_df = pd.DataFrame(data, columns=club_cols)

        return all_strava_club_activities_df
