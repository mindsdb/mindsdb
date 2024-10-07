import github

from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.github_handler.github_tables import (
    GithubIssuesTable,
    GithubPullRequestsTable,
    GithubCommitsTable,
    GithubReleasesTable,
    GithubBranchesTable,
    GithubContributorsTable,
    GithubMilestonesTable,
    GithubProjectsTable, GithubFilesTable
)

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)

class GithubHandler(APIHandler):
    """The GitHub handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the GitHub handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.repository = connection_data["repository"]
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        self._register_table("issues", GithubIssuesTable(self))
        self._register_table("pull_requests", GithubPullRequestsTable(self))
        self._register_table("commits", GithubCommitsTable(self))
        self._register_table("releases", GithubReleasesTable(self))
        self._register_table("branches", GithubBranchesTable(self))
        self._register_table("contributors", GithubContributorsTable(self))
        self._register_table("milestones", GithubMilestonesTable(self))
        self._register_table("projects", GithubProjectsTable(self))
        self._register_table("files", GithubFilesTable(self))

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """

        if self.is_connected is True:
            return self.connection

        connection_kwargs = {}

        if self.connection_data.get("api_key", None):
            connection_kwargs["login_or_token"] = self.connection_data["api_key"]

        if self.connection_data.get("github_url", None):
            connection_kwargs["base_url"] = self.connection_data["github_url"]

        self.connection = github.Github(**connection_kwargs)
        self.is_connected = True

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
            if self.connection_data.get("api_key", None):
                current_user = self.connection.get_user().name
                logger.info(f"Authenticated as user {current_user}")
            else:
                logger.info("Proceeding without an API key")

            current_limit = self.connection.get_rate_limit()
            logger.info(f"Current rate limit: {current_limit}")
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to GitHub API: {e}!")
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
