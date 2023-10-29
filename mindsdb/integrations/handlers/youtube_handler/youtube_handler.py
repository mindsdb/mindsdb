from mindsdb.integrations.handlers.youtube_handler.youtube_tables import YoutubeCommentsTable, YoutubeChannelsTable, YoutubeVideosTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql

from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

logger = get_log("integrations.youtube_handler")

class YoutubeHandler(APIHandler):
    """Youtube handler implementation"""

    def __init__(self, name=None, **kwargs):
        """Initialize the Youtube handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})

        self.parser = parse_sql
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.connection = None
        self.is_connected = False

        youtube_video_comments_data =YoutubeCommentsTable(self)
        self._register_table("comments", youtube_video_comments_data)

        youtube_channel_data = YoutubeChannelsTable(self)
        self._register_table("channels", youtube_channel_data)

        youtube_video_data = YoutubeVideosTable(self)
        self._register_table("videos", youtube_video_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.connection and self.is_connected is True:
            return self.connection
        # Create an OAuth 2.0 flow object.
        client_id = self.connection_data["youtube_client_token"]
        client_secret = self.connection_data["youtube_client_secret"]
        flow_config = {
            "installed": {
                "client_id": client_id,
                "project_id": "mindsdb-youtube",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_secret": client_secret,
                "redirect_uris": [
                    "https://cloud.mindsdb.com",
                    "http://localhost:8080/",
                ],
                "javascript_origins": [
                    "https://cloud.mindsdb.com",
                    "http://127.0.0.1:47334",
                ],
            }
        }

        flow = InstalledAppFlow.from_client_config(
            client_config=flow_config,
            scopes=["https://www.googleapis.com/auth/youtube.force-ssl"],
        )
        credentials = flow.run_local_server()
        youtube = build("youtube", "v3", credentials=credentials)
        self.connection = youtube
        return self.connection

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.
        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Youtube API: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> StatusResponse:
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


connection_args = OrderedDict(
    youtube_client_token={
        "type": ARG_TYPE.STR,
        "description": "Client Token for accessing Youtube Application API",
        "required": True,
        "label": "Youtube client token",
    },
    youtube_client_secret={
        "type": ARG_TYPE.STR,
        "description": "Client Secret for accessing Youtube Application API",
        "required": True,
        "label": "Youtube client secret",
    },
)

connection_args_example = OrderedDict(
    youtube_client_secret="<your_youtube_client_secret>",
    youtube_client_token="<your_youtube_client_token>",
)
