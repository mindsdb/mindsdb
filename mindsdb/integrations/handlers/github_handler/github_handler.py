# from collections import OrderedDict

import github

from mindsdb.integrations.handlers.github_handler.github_tables import GithubIssuesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

# from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql


logger = get_log("integrations.github_handler")


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
        self._register_table("issues", github_issues_data)

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

        return StatusResponse(True)

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


# connection_args = OrderedDict(
#     repository={
#         "type": ARG_TYPE.STR,
#         "description": "GitHub repository to pull data from (e.g. 'mindsdb/mindsdb')",
#     },
#     api_key={
#         "type": ARG_TYPE.STR,
#         "description": "Optional API key to be used with the GitHub API",
#     },
#     github_url={
#         "type": ARG_TYPE.STR,
#         "description": "Optional GitHub URL to connect to GitHub Enterprise",
#     },
# )

# connection_args_example = OrderedDict(repository="mindsdb/mindsdb")
