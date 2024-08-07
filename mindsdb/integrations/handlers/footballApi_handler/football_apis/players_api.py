import time
import pandas as pd


class PlayersApi:
    def __init__(self, client):
        self._client = client

    def get_players(self, **params) -> pd.DataFrame:
        """
        Calls the players API of API-Football.

            Parameters
            ----------
            **params:
                Additional keyword arguments representing parameters specific
                to the API method being called.

            Returns
            -------
            pd.DataFrame
                A Pandas DataFrame containing the retrieved player data.
        """
        league = params.get('league', None)
        season = params.get('season', None)
        team = params.get('team', None)
        id = params.get('id', None)
        search = params.get('search', None)
        page = params.get('page', None)

        try:
            page_required = page if page is not None else 1
            data = self._get_players_data(id=id, team=team, league=league, season=season,
                                          search=search, page_required=page_required)
            player_data = pd.DataFrame.from_dict([self._flatten_json(d) for d in data])
        except Exception as e:
            raise e
        return player_data

    def _get_players_data(self, id=None, team=None, league=None, season=None, page=1, search=None,
                          player_data_list=None, page_required=1):
        if player_data_list is None:
            player_data_list = []

        players = self._client.get_player(id=id, team=team, league=league, season=season, page=page, search=search)
        if players["errors"]:
            raise Exception(players["errors"])
        player_data_list.extend(players['response'])

        if players['paging']['current'] < page_required and page_required <= players['paging']['total']:
            next_page = players['paging']['current'] + 1
            if next_page % 2 == 1:
                time.sleep(2)  # to avoid api rate-limit
            player_data_list = self._get_players_data(id=id, team=team, league=league, season=season, page=next_page,
                                                      search=search,
                                                      player_data_list=player_data_list,
                                                      page_required=page_required)

        return player_data_list

    def _flatten_json(self, json_data, prefix=''):
        """
        Recursively flattens a nested JSON object into a flat dictionary.

        Args:
            json_data (dict): The JSON object to flatten.
            prefix (str, optional): A prefix to be added to flattened keys.

        Returns:
            dict: A dictionary where keys represent the flattened keys of the JSON object,
                and values are the corresponding values.
        """
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
