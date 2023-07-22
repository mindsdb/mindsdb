import os
import time
from typing import Dict

import pandas as pd
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
        football_api_players = FootballApiTable(self)
        self._register_table("get_players", football_api_players)

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

    def call_football_api(self, method_name: str = None, **player_params):
        if method_name == "get_players":
            return self._get_players(**player_params)
        raise NotImplementedError(f"Method name {method_name} not supported by Football API. ")

    def _get_players_data(self, client, id=None, team=None, league=None, season=None, page=1, search=None,
                          player_data_list=None):
        if player_data_list is None:
            player_data_list = []

        players = client.get_player(id=id, team=team, league=league, season=season, page=page, search=search)
        if players["errors"]:
            raise Exception(players["errors"])
        player_data_list.extend(players['response'])

        if players['paging']['current'] < players['paging']['total']:
            next_page = players['paging']['current'] + 1
            if next_page % 2 == 1:
                time.sleep(2)  # to avoid api rate-limit
            player_data_list = self._get_players_data(client, id=id, team=team, league=league, season=season, page=page,
                                                      search=search,
                                                      player_data_list=player_data_list)

        return player_data_list

    def _flatten_json(self, json_data, prefix=''):
        flattened_dict = {}
        for key, value in json_data.items():
            new_key = prefix + key if prefix else key
            if isinstance(value, dict):
                flattened_dict.update(self._flatten_json(value, new_key + '_'))
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    if isinstance(item, dict):
                        flattened_dict.update(self._flatten_json(item, new_key + f'_{index}_'))
                    else:
                        flattened_dict[new_key + f'_{index}'] = item
            else:
                flattened_dict[new_key] = value
        return flattened_dict

    def _get_players(self, **params) -> pd.DataFrame:
        client = self.connect()
        league = params.get('league', None)
        season = params.get('season', None)
        team = params.get('team', None)
        id = params.get('id', None)
        search = params.get('search', None)
        page = params.get('page', None)

        try:
            data = self._get_players_data(client, id=id, team=team, league=league, season=season, page=page,
                                          search=search)
            player_data = pd.DataFrame.from_dict([self._flatten_json(d) for d in data])
        except Exception as e:
            raise e
        return player_data
