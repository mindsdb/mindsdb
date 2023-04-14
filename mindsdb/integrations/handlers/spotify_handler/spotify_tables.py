import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log

from mindsdb_sql.parser import ast

logger = get_log("integrations.spotify_handler")


class SpotifyArtistsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        pass

class SpotifyAlbumsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        pass

class SpotifyPlaylistsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        pass