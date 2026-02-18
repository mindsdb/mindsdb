import requests
import pandas as pd
from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse, HandlerResponse as Response, RESPONSE_TYPE
from .hn_table import StoriesTable, CommentsTable, HNStoriesTable, JobStoriesTable, ShowStoriesTable

logger = log.getLogger(__name__)


class HackerNewsHandler(APIHandler):
    """
    A class for handling connections and interactions with the Hacker News API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        self.base_url = 'https://hacker-news.firebaseio.com/v0'

        stories = StoriesTable(self)
        self._register_table('stories', stories)

        hnstories = HNStoriesTable(self)
        self._register_table('hnstories', hnstories)

        jobstories = JobStoriesTable(self)
        self._register_table('jobstories', jobstories)

        showstories = ShowStoriesTable(self)
        self._register_table('showstories', showstories)

        comments = CommentsTable(self)
        self._register_table('comments', comments)

    def connect(self):
        return

    def check_connection(self) -> StatusResponse:
        try:
            response = requests.get(f'{self.base_url}/maxitem.json')
            response.raise_for_status()
            return StatusResponse(True)
        except Exception as e:
            logger.error(f'Error checking connection: {e}')
            return StatusResponse(False, str(e))

    def native_query(self, query_string: str = None):
        method_name, params = self.parse_native_query(query_string)

        df = self.call_hackernews_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def get_df_from_class(self, table: StoriesTable = None, limit: int = None):
        url = f'{self.base_url}/{table.json_endpoint}'
        response = requests.get(url)
        data = response.json()
        stories_data = []
        if limit is None:
            limit = len(data)
        for story_id in data[:limit]:
            url = f'{self.base_url}/item/{story_id}.json'
            response = requests.get(url)
            story_data = response.json()
            stories_data.append(story_data)
        return pd.DataFrame(stories_data, columns=table.columns)

    def call_hackernews_api(self, method_name: str = None, params: dict = None):
        story_method_handlers = {
            'get_top_stories': StoriesTable,
            'ask_hn_stories': HNStoriesTable,
            'get_job_stories': JobStoriesTable,
            'show_hn_stories': ShowStoriesTable,
        }
        if method_name in story_method_handlers:
            table = story_method_handlers[method_name]
            df = self.get_df_from_class(table)
        elif method_name == 'get_comments':
            item_id = params.get('item_id')
            url = f'{self.base_url}/item/{item_id}.json'
            response = requests.get(url)
            item_data = response.json()
            if 'kids' in item_data:
                comments_data = []
                for comment_id in item_data['kids']:
                    url = f'{self.base_url}/item/{comment_id}.json'
                    response = requests.get(url)
                    comment_data = response.json()
                    comments_data.append(comment_data)
                df = pd.DataFrame(comments_data)
            else:
                df = pd.DataFrame()
        else:
            raise ValueError(f'Unknown method_name: {method_name}')

        return df
