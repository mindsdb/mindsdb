from mindsdb.integrations.handlers.strava_handler.strava_tables import StravaAllClubsTable
from mindsdb.integrations.handlers.strava_handler.strava_tables import StravaClubActivitesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from stravalib.client import Client

logger = log.getLogger(__name__)


class StravaHandler(APIHandler):
    """Strava handler implementation"""

    def __init__(self, name=None, **kwargs):
        """Initialize the Strava handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})

        self.parser = parse_sql
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

        client = Client()
        client.access_token = self.connection_data['strava_access_token']
        self.connection = client

        return self.connection

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.
        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)

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
        ast = parse_sql(query)
        return self.query(ast)


connection_args = OrderedDict(
    strava_client_id={
        'type': ARG_TYPE.STR,
        'description': 'Client id for accessing Strava Application API'
    },
    strava_access_token={
        'type': ARG_TYPE.STR,
        'description': 'Access Token for accessing Strava Application API'
    }
)

connection_args_example = OrderedDict(
    strava_client_id='<your-strava-client_id>',
    strava_access_token='<your-strava-api-token>'
)
