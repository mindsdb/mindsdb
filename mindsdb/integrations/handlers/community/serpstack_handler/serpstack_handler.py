import requests

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.libs.api_handler import APIHandler

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response
)

from .serpstack_tables import (
    OrganicResultsTable, ImageResultsTable,
    VideoResultsTable, NewsResultsTable, ShoppingResultsTable
)


logger = log.getLogger(__name__)


class SerpstackHandler(APIHandler):
    """A class for handling connections and interactions with the Serpstack API.
    Attributes:
        api_key (str): API access key for the Serpstack API.
        is_connected (bool): Whether or not the API client is connected to Serpstack.
    """

    name = 'serpstack'

    def __init__(self, name: str = None, **kwargs):
        super().__init__(name)

        connection_data = kwargs.get('connection_data', {})
        self.connection_data = connection_data

        self.access_key = None
        self.base_url = None
        self.is_connected = False

        if 'access_key' in self.connection_data:
            self.access_key = self.connection_data['access_key']

        # register tables
        organic_results = OrganicResultsTable(self)
        self._register_table('organic_results', organic_results)

        image_results = ImageResultsTable(self)
        self._register_table('image_results', image_results)

        video_results = VideoResultsTable(self)
        self._register_table('video_results', video_results)

        news_results = NewsResultsTable(self)
        self._register_table('news_results', news_results)

        shopping_results = ShoppingResultsTable(self)
        self._register_table('shopping_results', shopping_results)

    def connect(self):
        """ Sets up connection and returns the base URL to be used"""

        if self.is_connected:
            return self.base_url

        if not self.access_key:
            logger.error("No access key provided for Serpstack API")
            return None

        try:
            url = f"https://api.serpstack.com/search?access_key={self.access_key}"
            api_request = requests.get(url)
            api_response = api_request.json()
            # error 105 means that user is on a free plan
            if api_response['error']['code'] == 105:
                self.base_url = "http://api.serpstack.com/search"
            # error 310 means that missing search query, which means that user can use https
            elif api_response['error']['code'] == 310:
                self.base_url = "https://api.serpstack.com/search"
            # any other error suggests issues with the account
            else:
                logger.error(f"Failed to connect to Serpstack API: {api_response['error']['info']}")
                return None

            self.is_connected = True
            return self.base_url

        except Exception as e:
            logger.error(f"Failed to connect to Serpstack API: {str(e)}")
            return None

    def check_connection(self) -> StatusResponse:
        """ Checks connection to Serpstack API"""
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
            response.copy_storage = True
        except Exception as e:
            response.error_message = (
                f"Failed to connect to Serpstack API: {str(e)}"
            )
            logger.error(response.error_message)
            response.success = False

        self.is_connected = response.success

        return response

    def native_query(self, query: str = None) -> Response:
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
