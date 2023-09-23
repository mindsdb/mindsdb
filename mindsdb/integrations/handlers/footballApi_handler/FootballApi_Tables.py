import pandas as pd
from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class FootballApiTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Football API

                Parameters
                ----------
                query : ast.Select
                   Given SQL SELECT query

                Returns
                -------
                pd.DataFrame
                    List of Football players matching the query

                Raises
                ------
                ValueError
                    If the query contains an unsupported condition
                """
        conditions = extract_comparison_conditions(query.where)
        player_params = {}
        for a_where in conditions:
            key = a_where[1]

            if a_where[0] != "=":
                raise ValueError(f"Unsupported where operation for {key}")

            # Assign values based on the key
            if key == "league":
                player_params["league"] = int(a_where[2])
            elif key == "season":
                player_params["season"] = int(a_where[2])
            elif key == "team":
                player_params["team"] = int(a_where[2])
            elif key == "id":
                player_params["id"] = int(a_where[2])
            elif key == "search":
                player_params["search"] = a_where[2]
            elif key == "page":
                player_params["page"] = int(a_where[2])
            else:
                raise ValueError(f"Unsupported where argument {key}")

        player_data = self.handler.call_football_api("get_players", **player_params)
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = player_data.columns
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(player_data) == 0:
            return pd.DataFrame([], columns=columns)

        # Remove columns not part of select.
        for col in set(player_data.columns).difference(set(columns)):
            player_data = player_data.drop(col, axis=1)

        return player_data

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""

        return [
            "player_id",
            "player_name",
            "player_firstname",
            "player_lastname",
            "player_age",
            "player_birth_date",
            "player_birth_place",
            "player_birth_country",
            "player_nationality",
            "player_height",
            "player_weight",
            "player_injured",
            "player_photo",
            "statistics_0_team_id",
            "statistics_0_team_name",
            "statistics_0_team_logo",
            "statistics_0_league_id",
            "statistics_0_league_name",
            "statistics_0_league_country",
            "statistics_0_league_logo",
            "statistics_0_league_flag",
            "statistics_0_league_season",
            "statistics_0_games_appearences",
            "statistics_0_games_lineups",
            "statistics_0_games_minutes",
            "statistics_0_games_number",
            "statistics_0_games_position",
            "statistics_0_games_rating",
            "statistics_0_games_captain",
            "statistics_0_substitutes_in",
            "statistics_0_substitutes_out",
            "statistics_0_substitutes_bench",
            "statistics_0_shots_total",
            "statistics_0_shots_on",
            "statistics_0_goals_total",
            "statistics_0_goals_conceded",
            "statistics_0_goals_assists",
            "statistics_0_goals_saves",
            "statistics_0_passes_total",
            "statistics_0_passes_key",
            "statistics_0_passes_accuracy",
            "statistics_0_tackles_total",
            "statistics_0_tackles_blocks",
            "statistics_0_tackles_interceptions",
            "statistics_0_duels_total",
            "statistics_0_duels_won",
            "statistics_0_dribbles_attempts",
            "statistics_0_dribbles_success",
            "statistics_0_dribbles_past",
            "statistics_0_fouls_drawn",
            "statistics_0_fouls_committed",
            "statistics_0_cards_yellow",
            "statistics_0_cards_yellowred",
            "statistics_0_cards_red",
            "statistics_0_penalty_won",
            "statistics_0_penalty_commited",
            "statistics_0_penalty_scored",
            "statistics_0_penalty_missed",
            "statistics_0_penalty_saved"
        ]



