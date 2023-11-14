import github
from collections import OrderedDict

from mindsdb.integrations.handlers.github_handler.github_tables import (
    GithubIssuesTable,
    GithubPullRequestsTable,
    GithubCommitsTable,
    GithubReleasesTable,
    GithubBranchesTable,
    GithubContributorsTable,
    GithubMilestonesTable,
    GithubProjectsTable
)

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.utilities import log
from mindsdb_sql import parse_sql


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

        github_issues_data = GithubIssuesTable(self)
        github_pull_requests_data = GithubPullRequestsTable(self)
        github_commits_data = GithubCommitsTable(self)
        github_releases_data = GithubReleasesTable(self)
        github_branches_data = GithubBranchesTable(self)
        github_contributors_data = GithubContributorsTable(self)
        github_milestones_data = GithubMilestonesTable(self)
        github_projects_data = GithubProjectsTable(self)

        self._register_table("issues", github_issues_data)
        self._register_table("pull_requests", github_pull_requests_data)
        self._register_table("commits", github_commits_data)
        self._register_table("releases", github_releases_data)
        self._register_table("branches", github_branches_data)
        self._register_table("contributors", github_contributors_data)
        self._register_table("milestones", github_milestones_data)
        self._register_table("projects", github_projects_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """

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


connection_args = OrderedDict(
    repository={
        "type": ARG_TYPE.STR,
        "description": " GitHub repository name.",
        "required": True,
        "label": "Repository",
    },
    api_key={
        "type": ARG_TYPE.PWD,
        "description": "Optional GitHub API key to use for authentication.",
        "required": False,
        "label": "Api key",
    },
    github_url={
        "type": ARG_TYPE.STR,
        "description": "Optional GitHub URL to connect to a GitHub Enterprise instance.",
        "required": False,
        "label": "Github url",
    },
)

connection_args_example = OrderedDict(
    repository="mindsdb/mindsdb", 
    api_key="ghp_z91InCQZWZAMlddOzFCX7xHJrf9Fai35HT7", 
    github_url="https://github.com/mindsdb/mindsdb"
)
