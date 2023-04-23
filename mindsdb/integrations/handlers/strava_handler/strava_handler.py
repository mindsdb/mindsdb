from mindsdb.integrations.handlers.strava_handler.strava_tables import StravaAllClubsTable
from mindsdb.integrations.handlers.strava_handler.strava_tables import StravaClubActivitesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql

import requests
import pandas as pd
import json

logger = get_log("integrations.stravahandler")

class StravaHandler(APIHandler):
    """Strava handler implementation"""

    def __init__(self, name=None, **kwargs):
        """Initialize the Confluence handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})

        self.parser = parse_sql
        self.dialect = 'strava'
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.connection = None
        self.is_connected = False

        strava_all_clubs_data = StravaAllClubsTable(self)
        self._register_table("all_clubs", strava_all_clubs_data)
        
        strava_club_activites_data = StravaClubActivitesTable(self)
        self._register_table("club_activities", strava_club_activites_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection

        strava_url = f"https://www.strava.com/api/v3/athlete/clubs"
        headers = {
            "Authorization": f"Bearer {self.connection_data['strava_api_token']}",
        }
        response = requests.request("GET",strava_url,headers=headers)

        if response.status_code == 200:
            self.connection = response
        else:
            raise Exception('Not able to connect with Strava API. Possibly wrong API Key')


        return self.connection

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.
        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Strava API: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query.
        Parameters
        ----------
        query : str
            query in a native format
        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)

        