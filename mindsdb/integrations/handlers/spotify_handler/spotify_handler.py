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

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd


logger = get_log("integrations.spotify_handler")


class SpotifHandler(APIHandler):
    """A class for handling connections and interactions with the Spotify API.

    Attributes:
        bearer_token (str): The consumer key for the Spotify app.
        api (tweepy.API): The `tweepy.API` object for interacting with the Spotify API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.api = None
        self.Bearer = None
        self.is_connected = False

        args = kwargs.get('connection_data', {})
        self.client_id = args.get('client_id')
        self.client_secret = args.get('client_secret')

        if 'Bearer' in args:
            self.Bearer = args['Bearer']

    def create_connection(self):

        sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret
        ))
        return sp

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
        """Check the status of the Spotify API connection.

        Returns:
            StatusResponse: The status of the connection.
        """
        response = StatusResponse(False)
        try:
            self.connect()
            if self.connection_data.get("Bearer", None):
                logger.info("Authenticated")
            else:
                logger.info("Proceeding without an API key")

            current_limit = self.connection.get_rate_limit()
            logger.info(f"Current rate limit: {current_limit}")
            response.success = True
        except spotipy.exceptions.SpotifyException as e:
            logger.error(f"Error connecting to Spotify API: {e}!")
            response.error_message = str(e)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to Spotify API: {e}!")
            response.error_message = str(e)

        self.is_connected = response.success

        return response

    def _get_albums(self, params: Dict = None) -> pd.DataFrame:
        df = pd.DataFrame([])
        return df

    def _get_artists(self, params: Dict = None) -> pd.DataFrame:
        df = pd.DataFrame([])
        return df

    def _get_playlists(self, params: Dict = None) -> pd.DataFrame:
        df = pd.DataFrame([])
        return df

    def call_spotify_api(self, method_name: str = None, params: dict = None, filters: list = None):
        """Calls the Spotify API method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call (e.g. artits)
            params (Dict): Params to pass to the API call
        """
        if method_name == 'albums':
            return self._get_albums(params)
        if method_name == 'artists':
            return self._get_artists(params)
        if method_name == 'playlists':
            return self._get_playlists(params)
        raise NotImplementedError('Method name {} not supported by Binance API Handler'.format(method_name))

    def native_query(self, query: str = None):
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
