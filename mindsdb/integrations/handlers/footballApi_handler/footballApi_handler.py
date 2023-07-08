import os
from typing import Dict

from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.footballApi_handler.FootballApi_Tables import FootballApiTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse, HandlerResponse,
)
from mindsdb.utilities import log
from footballAPIClient import footballAPI, FootballAPI


class FootballApiHandler(APIHandler):

    def __init__(self, name: str = None, **kwargs):
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
        football_api_teams_info = FootballApiTable(self)
        self._register_table("get_teams_info", football_api_teams_info)

    def connect(self) -> FootballAPI:
        if self.is_connected is True:
            return self.connection

        football_client = footballAPI.FootballAPI(self.account_type, self.api_key)
        self.connection = football_client
        self.is_connected = True
        return self.connection

    def check_connection(self) -> StatusResponse:
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
        ast = parse_sql(query, dialect='mindsdb')
        return self.query(ast)

    def call_football_api(self, method_name: str = None, params: Dict = None):
        if method_name == "get_team_statistics":
            return self._get_team_statistics(params)
        raise NotImplementedError(f"Method name {method_name} not supported by Football API. ")

    def _get_team_statistics(self, params: Dict = None):
        client = self.connect()
        league = params.get('league', None),
        season = params.get('season', None)
        team = params.get('team', None)
        date = params.get('date', None)

        try:
            data = client.get_team_statistics(league=league, season=season, team=team, date=date)
        except Exception as e:
            raise e
        return data
