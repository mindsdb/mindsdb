from mindsdb.integrations.handlers.github_handler.github_tables import SpotifyAritistsTable, SpotifyAlbumsTable, SpotifyPlaylistsTale
from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class SpotifHandler(APIHandler):
    """A class for handling connections and interactions with the Spotify API.

    Attributes:
        bearer_token (str): The consumer key for the Spotify app.
        api (tweepy.API): The `tweepy.API` object for interacting with the Spotify API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})
        self.connection_args = {}

        handler_config = Config().get('spotify_handler', {})
        self.api = None
        self.is_connected = False

        self._tables = {}

    def create_connection(self, api_version=2):
        pass

    def connect(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
        HandlerStatusResponse
        """


        pass

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)
        pass

    def call_spotify_api(self, method_name: str = None, params: dict = None, filters: list = None):
        pass

    def _register_table(self, table_name: str, table_class: Any):
        self._tables[table_name] = table_class

        pass
