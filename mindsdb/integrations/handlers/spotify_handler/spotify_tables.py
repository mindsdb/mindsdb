import logging
import pandas as pd

from typing import List
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log

from mindsdb_sql.parser import ast

logger = get_log("integrations.spotify_handler")


logger = logging.getLogger(__name__)


class SpotifyTracksTable(APITable):
    """The Spotify Tracks Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Spotify "Search tracks" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Spotify tracks matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 20

        tracks_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "tracks":
                    next

                if an_order.field.parts[1] in ["name", "artist", "album", "duration"]:
                    if tracks_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for name/artist/album/duration"
                        )

                    tracks_kwargs["sort"] = an_order.field.parts[1]
                    tracks_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "search":
                if a_where[0] != "LIKE":
                    raise ValueError("Unsupported where operation for search")
                tracks_kwargs["q"] = a_where[2]

                continue
            elif a_where[1] == "artist":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for artist")
                tracks_kwargs["artist"] = a_where[2]

                continue
            elif a_where[1] == "album":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for album")
                tracks_kwargs["album"] = a_where[2]

                continue
            elif a_where[1] == "duration":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for duration")
                tracks_kwargs["duration"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        self.handler.connect()

        spotify_tracks_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:
                for a_track in self.handler.connection.search_tracks(**tracks_kwargs, offset=start, limit=10):
                    logger.debug(f"Processing track {a_track.name}")

                    spotify_tracks_df = pd.concat(
                        [
                            spotify_tracks_df,
                            pd.DataFrame(
                                [
                                    {
                                        "id": a_track.id,
                                        "name": a_track.name,
                                        "artist": a_track.artist,
                                        "album": a_track.album,
                                        "duration": a_track.duration,
                                        "popularity": a_track.popularity,
                                    }
                                ]
                            ),
                        ]
                    )

                    if spotify_tracks_df.shape[0] >= total_results:
                        break
            except IndexError:
                break


class SpotifyArtistsTable(APITable):
    """The Spotify Artists Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Spotify "Search artists" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Spotify artists matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 20

        artists_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "artists":
                    next

                if an_order.field.parts[1] in ["name", "genre", "popularity"]:
                    if artists_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for name/genre/popularity"
                        )

                    artists_kwargs["sort"] = an_order.field.parts[1]
                    artists_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "search":
                if a_where[0] != "LIKE":
                    raise ValueError("Unsupported where operation for search")
                artists_kwargs["q"] = a_where[2]

                continue
            elif a_where[1] == "genre":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for genre")
                artists_kwargs["genre"] = a_where[2]

                continue
            elif a_where[1] == "popularity":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for popularity")
                artists_kwargs["popularity"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        self.handler.connect()

        spotify_artists_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:
                for an_artist in self.handler.connection.search_artists(**artists_kwargs, offset=start, limit=10):
                    logger.debug(f"Processing artist {an_artist.name}")

                    spotify_artists_df = pd.concat(
                        [
                            spotify_artists_df,
                            pd.DataFrame(
                                [
                                    {
                                        "id": an_artist.id,
                                        "name": an_artist.name,
                                        "genre": an_artist.genre,
                                        "popularity": an_artist.popularity,
                                    }
                                ]
                            ),
                        ]
                    )

                    if spotify_artists_df.shape[0] >= total_results:
                        break
            except IndexError:
                break


 class SpotifyAlbumsTable(APITable):
    """The Spotify Albums Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Spotify "Search albums" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Spotify albums matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 20

        albums_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "albums":
                    next

                if an_order.field.parts[1] in ["name", "artist", "release_date"]:
                    if albums_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for name/artist/release_date"
                        )

                    albums_kwargs["sort"] = an_order.field.parts[1]
                    albums_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "search":
                if a_where[0] != "LIKE":
                    raise ValueError("Unsupported where operation for search")
                albums_kwargs["q"] = a_where[2]

                continue
            elif a_where[1] == "artist":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for artist")
                albums_kwargs["artist"] = a_where[2]

                continue
            elif a_where[1] == "release_date":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for release_date")
                albums_kwargs["release_date"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        self.handler.connect()

        spotify_albums_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:
                for an_album in self.handler.connection.search_albums(**albums_kwargs, offset=start, limit=10):
                    logger.debug(f"Processing album {an_album.name}")

                    spotify_albums_df = pd.concat(
                        [
                            spotify_albums_df,
                            pd.DataFrame(
                                [
                                    {
                                        "id": an_album.id,
                                        "name": an_album.name,
                                        "artist": an_album.artist,
                                        "release_date": an_album.release_date,
                                        "popularity": an_album.popularity,
                                    }
                                ]
                            ),
                        ]
                    )

                    if spotify_albums_df.shape[0] >= total_results:
                        break
            except IndexError:
                break



class SpotifyPlaylistsTable(APITable):
    """The Spotify Playlists Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Spotify "Search playlists" API

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Spotify playlists matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 20

        playlists_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "playlists":
                    next

                if an_order.field.parts[1] in ["name", "owner", "followers"]:
                    if playlists_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for name/owner/followers"
                        )

                    playlists_kwargs["sort"] = an_order.field.parts[1]
                    playlists_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "search":
                if a_where[0] != "LIKE":
                    raise ValueError("Unsupported where operation for search")
                playlists_kwargs["q"] = a_where[2]

                continue
            elif a_where[1] == "owner":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for owner")
                playlists_kwargs["owner"] = a_where[2]

                continue
            elif a_where[1] == "followers":
                if a_where[0] != ">=":
                    raise ValueError("Unsupported where operation for followers")
                playlists_kwargs["followers"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        self.handler.connect()

        spotify_playlists_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:
                for a_playlist in self.handler.connection.search_playlists(**playlists_kwargs, offset=start, limit=10):
                    logger.debug(f"Processing playlist {a_playlist.name}")

                    spotify_playlists_df = pd.concat(
                        [
                            spotify_playlists_df,
                            pd.DataFrame(
                                [
                                    {
                                        "id": a_playlist.id,
                                        "name": a_playlist.name,
                                        "owner": a_playlist.owner,
                                        "followers": a_playlist.followers,
                                        "tracks": a_playlist.tracks,
                                    }
                                ]
                            ),
                        ]
                    )

                    if spotify_playlists_df.shape[0] >= total_results:
                        break
            except IndexError:
                break
