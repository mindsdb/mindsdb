from collections import OrderedDict

from mindsdb.integrations.handlers.clipdrop_handler.clipdrop_tables import (
    DockerHubRepoImagesSummaryTable,
    DockerHubRepoImagesTable,
    DockerHubRepoTagTable,
    DockerHubRepoTagsTable,
    DockerHubOrgSettingsTable
)
from mindsdb.integrations.handlers.clipdrop_handler.clipdrop import ClipdropClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql


logger = get_log("integrations.clipdrop_handler")


class ClipdropHandler(APIHandler):
    """The Clipdrop handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the Clipdrop handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.client = ClipdropClient()
        self.is_connected = False

        repo_images_stats_data = ClipdropRepoImagesSummaryTable(self)
        self._register_table("repo_images_summary", repo_images_stats_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        status = self.client.login(self.connection_data.get("username"), self.connection_data.get("password"))
        if status["code"] != 200:
            resp.success = False
            resp.error_message = status["error"]
            return resp
        self.is_connected = True
        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)

        try:
            status = self.client.login(self.connection_data.get("username"), self.connection_data.get("password"))
            if status["code"] == 200:
                current_user = self.connection_data.get("username")
                logger.info(f"Authenticated as user {current_user}")
                response.success = True
            else:
                response.success = False
                logger.info("Error connecting to dockerhub. " + status["error"])
                response.error_message = status["error"]
        except Exception as e:
            logger.error(f"Error connecting to Clipdrop API: {e}!")
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
    api_key={
        "type": ARG_TYPE.STR,
        "description": "Clipdrop API key",
        "required": True,
        "label": "api_key",
    }
    dir_to_save={
        "type": ARG_TYPE.STR,
        "description": "The local directory to save Clipdrop API response",
        "required": True,
        "label": "api_key",
    }
)

connection_args_example = OrderedDict(
    api_key="api_key",
    dir_to_save="/Users/sam/Downloads"
)
