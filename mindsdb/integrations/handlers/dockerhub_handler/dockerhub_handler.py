from mindsdb.integrations.handlers.dockerhub_handler.dockerhub_tables import (
    DockerHubRepoImagesSummaryTable,
    DockerHubRepoImagesTable,
    DockerHubRepoTagTable,
    DockerHubRepoTagsTable,
    DockerHubOrgSettingsTable
)
from mindsdb.integrations.handlers.dockerhub_handler.dockerhub import DockerHubClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class DockerHubHandler(APIHandler):
    """The DockerHub handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the DockerHub handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.docker_client = DockerHubClient()
        self.is_connected = False

        repo_images_stats_data = DockerHubRepoImagesSummaryTable(self)
        self._register_table("repo_images_summary", repo_images_stats_data)

        repo_images_data = DockerHubRepoImagesTable(self)
        self._register_table("repo_images", repo_images_data)

        repo_tag_details_data = DockerHubRepoTagTable(self)
        self._register_table("repo_tag_details", repo_tag_details_data)

        repo_tags_data = DockerHubRepoTagsTable(self)
        self._register_table("repo_tags", repo_tags_data)

        org_settings = DockerHubOrgSettingsTable(self)
        self._register_table("org_settings", org_settings)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        status = self.docker_client.login(self.connection_data.get("username"), self.connection_data.get("password"))
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
            status = self.docker_client.login(self.connection_data.get("username"), self.connection_data.get("password"))
            if status["code"] == 200:
                current_user = self.connection_data.get("username")
                logger.info(f"Authenticated as user {current_user}")
                response.success = True
            else:
                response.success = False
                logger.info("Error connecting to dockerhub. " + status["error"])
                response.error_message = status["error"]
        except Exception as e:
            logger.error(f"Error connecting to DockerHub API: {e}!")
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
