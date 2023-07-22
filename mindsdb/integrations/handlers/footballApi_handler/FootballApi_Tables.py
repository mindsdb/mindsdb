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
            if a_where[1] == "league":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for state")
                player_params["league"] = int(a_where[2])
                continue

            if a_where[1] == "season":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for labels")
                player_params["season"] = int(a_where[2])
                continue

            if a_where[1] in "team":
                if a_where[0] != "=":
                    raise ValueError(f"Unsupported where operation for {a_where[1]}")
                player_params["team"] = int(a_where[2])

            if a_where[1] in "id":
                if a_where[0] != "=":
                    raise ValueError(f"Unsupported where operation for {a_where[1]}")
                player_params["id"] = int(a_where[2])

            if a_where[1] in "search":
                if a_where[0] != "=":
                    raise ValueError(f"Unsupported where operation for {a_where[1]}")
                player_params["search"] = a_where[2]

            if a_where[1] in "page":
                if a_where[0] != "=":
                    raise ValueError(f"Unsupported where operation for {a_where[1]}")
                player_params["page"] = int(a_where[2])
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

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


