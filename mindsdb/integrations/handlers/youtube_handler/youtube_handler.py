import os
import requests
from shutil import copyfile

from mindsdb.integrations.handlers.youtube_handler.youtube_tables import (
    YoutubeCommentsTable,
    YoutubeChannelsTable,
    YoutubeVideosTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql

from collections import OrderedDict
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from mindsdb.integrations.handlers.gmail_handler.utils import google_auth_flow, save_creds_to_file

DEFAULT_SCOPES = [
	'https://www.googleapis.com/auth/youtube',
	'https://www.googleapis.com/auth/youtube.force-ssl',
	'https://www.googleapis.com/auth/youtubepartner'
]

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
        
        creds = self._get_oauth2_credentials()

        youtube = build(
            "youtube", "v3", developerKey=self.youtube_api_token, credentials=creds
        )
        self.connection = youtube

        return self.connection
    
    def _get_oauth2_credentials(self):
        """Get OAuth2 credentials for the handler.
        Returns
        -------
        Credentials
            OAuth2 credentials
        """
        creds = None

        if self.credentials_file or self.credentials_url:
            try:
                # Get the current dir, we'll check for Token & Creds files in this dir
                curr_dir = self.handler_storage.folder_get('config')

                creds_file = os.path.join(curr_dir, 'creds.json')
                secret_file = os.path.join(curr_dir, 'secret.json')

                if os.path.isfile(creds_file):
                    creds = Credentials.from_authorized_user_file(creds_file, self.scopes)

                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())

                    if self._download_secret_file(secret_file):
                        # save to storage
                        self.handler_storage.folder_sync('config')
                    else:
                        raise ValueError('No valid Gmail Credentials filepath or S3 url found.')

                    creds = google_auth_flow(secret_file, self.scopes, self.connection_data.get('code'))

                    save_creds_to_file(creds, creds_file)
                    self.handler_storage.folder_sync('config')
            except Exception as e:
                logger.error(f"OAuth2 credentials not available: {e}!")

        return creds        
    
    def _download_secret_file(self, secret_file):
        # Giving more priority to the S3 file
        if self.credentials_url:
            response = requests.get(self.credentials_url)
            if response.status_code == 200:
                with open(secret_file, 'w') as creds:
                    creds.write(response.text)
                return True
            else:
                logger.error("Failed to get credentials from S3", response.status_code)

        if self.credentials_file and os.path.isfile(self.credentials_file):
            copyfile(self.credentials_file, secret_file)
            return True
        return False

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
    youtube_access_token={
        "type": ARG_TYPE.STR,
        "description": "API Token for accessing Youtube Application API",
        "required": True,
        "label": "Youtube access token",
    }
)

connection_args_example = OrderedDict(youtube_api_token="<your-youtube-api-token>")
