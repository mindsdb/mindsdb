from mindsdb.integrations.handlers.github_handler.github_tables import SpotifyAritistsTable, SpotifyAlbumsTable, SpotifyPlaylistsTale
from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
import requests
import json
from requests.structures import CaseInsensitiveDict
from typing import Optional

from mindsdb.utilities import log
from mindsdb.utilities.log import get_log
from mindsdb.utilities.config import Config

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from mindsdb_sql import parse_sql


logger = get_log("integrations.spotify_handler")


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
        spotify_albums_data = SpotifyAlbumsTable(self)
        spotify_artists_data = SpotifyAritistsTable(self)
        spotify_playlists_data = SpotifyPlaylistsTale(self)

        self._register_table("albums", spotify_albums_data)
        self._register_table("artists", spotify_artists_data)
        self._register_table("playlists", spotify_playlists_data)

    def create_connection(self):
        root = "https://accounts.spotify.com/api/token"
        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = f"grant_type=client_credentials&client_id={self.connection_args['client_id']}&client_secret={self.connection_args['client_secret']}"
        response = requests.post(root, data=data, headers=headers)
        response_json = json.loads(response.text)
        access_token = response_json.get("access_token")
        return access_token

    def connect(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
        HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.api
        self.api = self.create_connection()
        self.is_connected = True

        return self.api

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)
        try:
            self.connect()
            if self.connection_data.get("api_key", None):
                current_user = self.connection.get_user().name
                logger.info(f"Authenticated as user {current_user}")
            else:
                logger.info("Proceeding without an API key")

            current_limit = self.connection.get_rate_limit()
            logger.info(f"Current rate limit: {current_limit}")
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Spotify API: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response

    def call_spotify_api(self, method_name: str = None, params: dict = None, filters: list = None):
        pass

    def _register_table(self, table_name: str, table_class: Any):
        self._tables[table_name] = table_class

        pass

    def native_query(self, query: str = None):
        method_name, params = FuncParser().from_string(query)
        df = self.call_spotify_api(method_name, params)
        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )
