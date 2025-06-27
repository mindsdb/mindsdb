import praw
import os
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.config import Config
from mindsdb.utilities import log

from .reddit_tables import CommentTable, SubmissionTable

logger = log.getLogger(__name__)


class RedditHandler(APIHandler):

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = Config().get('reddit_handler', {})
        for k in ['client_id', 'client_secret', 'user_agent']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'REDDIT_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'REDDIT_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.reddit = None
        self.is_connected = False

        comment = CommentTable(self)
        self._register_table('comment', comment)

        submission = SubmissionTable(self)
        self._register_table('submission', submission)

    def connect(self):
        """Authenticate with the Reddit API using the client ID, client secret
        and user agent provided in the constructor.
        """
        if self.is_connected is True:
            return self.reddit

        self.reddit = praw.Reddit(
            client_id=self.connection_args['client_id'],
            client_secret=self.connection_args['client_secret'],
            user_agent=self.connection_args['user_agent'],
        )

        self.is_connected = True
        return self.reddit

    def check_connection(self) -> StatusResponse:
        '''It evaluates if the connection with Reddit API is alive and healthy.
        Returns:
            HandlerStatusResponse
        '''

        response = StatusResponse(False)

        try:
            reddit = self.connect()
            reddit.user.me()
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Reddit api: {e}. '
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        '''It parses any native statement string and acts upon it (for example, raw syntax commands).
        Args:
        query (Any): query in native format (str for sql databases,
            dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        '''

        method_name, params = self.parse_native_query(query_string)
        if method_name == 'get_submission':
            df = self.get_submission(params)
        elif method_name == 'get_subreddit':
            df = self.get_subreddit(params)
        else:
            raise ValueError(f"Method '{method_name}' not supported by RedditHandler")

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )
