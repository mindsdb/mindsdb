from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler_generator import APIResourceGenerator


logger = log.getLogger(__name__)


class GithubHandler(APIHandler):

    def __init__(self, name=None, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})

        self.is_connected = False

        self.api_resource_generator = APIResourceGenerator(
            "https://raw.githubusercontent.com/github/rest-api-description/refs/heads/main/descriptions/api.github.com/api.github.com.yaml",
            self.connection_data,
            url_base='/repos/{owner}/{repo}/',
            options={
                'page_num_param': ['page'],
                'page_size_param': ['per_page'],
            }
        )

        resource_tables = self.api_resource_generator.generate_api_resources(self)

        for table_name, resource in resource_tables.items():
            self._register_table(table_name, resource)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """
        return

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            self.api_resource_generator.check_connection()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to GitHub API: {e}!")
            response.error_message = e

        self.is_connected = response.success
        return response
