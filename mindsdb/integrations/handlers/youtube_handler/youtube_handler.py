from mindsdb.integrations.handlers.youtube_handler.youtube_tables import (
    YoutubeCommentsTable,
    YoutubeChannelsTable,
    YoutubeVideosTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

from mindsdb.utilities.config import Config

from googleapiclient.discovery import build

from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleUserOAuth2Manager

DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/youtube',
    'https://www.googleapis.com/auth/youtube.force-ssl',
    'https://www.googleapis.com/auth/youtubepartner'
]

logger = log.getLogger(__name__)


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
        self.connection_data = kwargs.get("connection_data", {})
        self.kwargs = kwargs

        self.parser = parse_sql
        self.connection = None
        self.is_connected = False

        self.handler_storage = kwargs['handler_storage']

        self.credentials_url = self.connection_data.get('credentials_url', None)
        self.credentials_file = self.connection_data.get('credentials_file', None)
        if self.connection_data.get('credentials'):
            self.credentials_file = self.connection_data.pop('credentials')
        if not self.credentials_file and not self.credentials_url:
            # try to get from config
            yt_config = Config().get('handlers', {}).get('youtube', {})
            secret_file = yt_config.get('credentials_file')
            secret_url = yt_config.get('credentials_url')
            if secret_file:
                self.credentials_file = secret_file
            elif secret_url:
                self.credentials_url = secret_url

        self.youtube_api_token = self.connection_data.get('youtube_api_token', None)

        self.scopes = self.connection_data.get('scopes', DEFAULT_SCOPES)

        youtube_video_comments_data = YoutubeCommentsTable(self)
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
        if self.is_connected is True:
            return self.connection

        google_oauth2_manager = GoogleUserOAuth2Manager(self.handler_storage, self.scopes, self.credentials_file, self.credentials_url, self.connection_data.get('code'))
        creds = google_oauth2_manager.get_oauth2_credentials()

        youtube = build(
            "youtube", "v3", developerKey=self.youtube_api_token, credentials=creds
        )
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

        try:
            self.connect()
            response.success = True
            response.copy_storage = True
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
        ast = parse_sql(query)
        return self.query(ast)
