from mindsdb.integrations.handlers.youtube_handler.youtube_tables import YoutubeGetCommentsTable, YoutubeGetVideoTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql

from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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

        youtube_video_comments_data =YoutubeGetCommentsTable(self)
        self._register_table("get_comments", youtube_video_comments_data)

        self._register_table("get_video_resource", YoutubeGetVideoTable(self))

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection

        youtube = build('youtube', 'v3', developerKey=self.connection_data['youtube_api_token'])
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

def get_video_resource(self, video_id):
    """Extracts the Video Resource data for a given video ID.

    Args:
        video_id: The ID of the video.

    Returns:
        None
    """
    youtube = self.connect()

    request = youtube.videos().list(part='snippet', id=video_id)
    response = request.execute()

    video_resource = response['items'][0]

    # Insert the video resource data into the table
    table = self.get_table("get_video_resource")
    table.insert_row(
        video_id=video_resource['id'],
        title=video_resource['snippet']['title'],
        description=video_resource['snippet']['description'],
        publish_date=video_resource['snippet']['publishedAt'],
        view_count=video_resource['statistics']['viewCount'],
        like_count=video_resource['statistics']['likeCount'],
        dislike_count=video_resource['statistics']['dislikeCount'],
        comment_count=video_resource['statistics']['commentCount'],
    )

connection_args = OrderedDict(
    youtube_access_token={
        'type': ARG_TYPE.STR,
        'description': 'API Token for accessing Youtube Application API',
        'required': True,
        'label': 'Youtube access token',
    }   
)

connection_args_example = OrderedDict(
    youtube_api_token ='<your-youtube-api-token>'
)
