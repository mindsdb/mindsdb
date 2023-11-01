import os

import pandas as pd
from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.footballApi_handler.footballapi_constants import FOOTBALL_API_CLIENT_METHODS
from mindsdb.integrations.handlers.footballApi_handler.players_table import PlayersTable
from mindsdb.integrations.handlers.footballApi_handler.football_apis.players_api import PlayersApi
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse, HandlerResponse,
)
from mindsdb.utilities import log
from footballAPIClient import footballAPI, FootballAPI


class FootballApiHandler(APIHandler):
    """The Football API handler implementation"""

    def __init__(self, name: str = None, **kwargs):
        """Initialize the Football API handler.

                Parameters
                ----------
                name : str
                    name of a handler instance
                """
        super().__init__(name)
        self.api_key = None
        self.account_type = None

        connection_data = kwargs.get('connection_data', {})
        if 'api_key' in connection_data:
            self.api_key = connection_data['api_key']
        elif 'API_KEY' in os.environ:
            self.api_key = os.environ['API_KEY']
        if 'account_type' in connection_data:
            self.account_type = connection_data['account_type']

        self.is_connected = False
        self.connection_data = connection_data
        self.connection = None
        players = PlayersTable(self)
        self._register_table("get_players", players)

    def connect(self) -> FootballAPI:
        """Set up the connection required by the handler.

                Returns
                -------
                StatusResponse
                    connection object
                """

        if self.is_connected is True:
            return self.connection

        football_client = footballAPI.FootballAPI(self.account_type, self.api_key)
        self.connection = football_client
        self.is_connected = True
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
            client = self.connect()
            client.get_status()
            response.success = True
        except Exception as e:
            log.logger.error(f"Error Connecting to Football API: {e}")
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> HandlerResponse:
        """Receive and process a raw query.

                Parameters
                ----------
                query : str
                    query in a native format

                Returns
                -------
                HandlerResponse
                    Request status
                """

        ast = parse_sql(query, dialect='mindsdb')
        return self.query(ast)

    def call_football_api(self, method_name: str, **params) -> pd.DataFrame:
        """Calls the Football API.

                        Parameters
                        ----------
                        method_name : str
                            name of the calling method from Football api
                        **params:
                            Additional keyword arguments representing parameters specific
                            to the API method being called.

                        Returns
                        -------
                        pd.DataFrame
                            A Pandas DataFrame containing the retrieved player data.
                        """
        if method_name == FOOTBALL_API_CLIENT_METHODS.get("PLAYERS"):
            client = self.connect()
            players_api = PlayersApi(client)
            return players_api.get_players(**params)
        raise NotImplementedError(f"Method name {method_name} not supported by Football API Handler. ")