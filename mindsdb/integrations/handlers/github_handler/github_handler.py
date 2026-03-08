import threading
from datetime import datetime, timedelta, timezone

import github
import requests
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.github_handler.github_tables import (
    GithubIssuesTable,
    GithubPullRequestsTable,
    GithubCommitsTable,
    GithubReleasesTable,
    GithubBranchesTable,
    GithubContributorsTable,
    GithubProjectsTable,
    GithubMilestonesTable,
    GithubFilesTable,
    GithubCheckRunsTable,
    GithubCommitStatusesTable,
    GithubPagesBuildsTable,
    GithubRepositoriesTable,
)
from mindsdb.integrations.handlers.github_handler.generate_api import get_github_types, get_github_methods, GHTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.utilities.sql_utils import FilterOperator
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)

GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"


class GithubHandler(APIHandler):
    """The GitHub handler implementation"""

    _refresh_lock = threading.Lock()

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
        self.repository = connection_data.get("repository")
        self.handler_storage = kwargs.get("handler_storage")
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        # custom tables
        self._register_table("issues", GithubIssuesTable(self))
        self._register_table("pulls", GithubPullRequestsTable(self))
        self._register_table("commits", GithubCommitsTable(self))
        self._register_table("releases", GithubReleasesTable(self))
        self._register_table("branches", GithubBranchesTable(self))
        self._register_table("contributors", GithubContributorsTable(self))
        self._register_table("projects", GithubProjectsTable(self))
        self._register_table("milestones", GithubMilestonesTable(self))
        self._register_table("files", GithubFilesTable(self))
        self._register_table("check_runs", GithubCheckRunsTable(self))
        self._register_table("commit_statuses", GithubCommitStatusesTable(self))
        self._register_table("pages_builds", GithubPagesBuildsTable(self))
        self._register_table("repositories", GithubRepositoriesTable(self))

        # generated tables
        github_types = get_github_types()

        # generate tables from repository object
        for method in get_github_methods(github.Repository.Repository):
            if method.table_name in self._tables:
                continue

            table = GHTable(self, github_types=github_types, method=method)
            self._register_table(method.table_name, table)

    def _uses_oauth(self):
        return bool(
            self.connection_data.get("access_token")
            or self.connection_data.get("refresh_token")
        )

    def get_repos(self, conditions=None):
        """Resolve repos from WHERE conditions or connection default.

        Supports:
          WHERE repository = 'owner/repo'
          WHERE repository IN ('owner/repo1', 'owner/repo2')
        Falls back to self.repository from connection_data.
        Returns a list of PyGithub Repository objects.
        """
        repo_names = []
        if conditions:
            for condition in conditions:
                if condition.column != "repository":
                    continue
                if condition.op == FilterOperator.EQUAL:
                    repo_names = [condition.value]
                    condition.applied = True
                elif condition.op == FilterOperator.IN:
                    repo_names = list(condition.value)
                    condition.applied = True
                break
        if not repo_names:
            if self.repository:
                repo_names = [self.repository]
            else:
                logger.warning("No repository specified in query. Returning empty result.")
                return []
        self.connect()
        return [self.connection.get_repo(name) for name in repo_names]

    def _load_stored_tokens(self):
        if not self.handler_storage:
            return None
        try:
            token_data = self.handler_storage.encrypted_json_get("github_tokens")
            if token_data and isinstance(token_data.get("expires_at"), str):
                token_data["expires_at"] = datetime.fromisoformat(token_data["expires_at"])
            return token_data
        except Exception:
            return None

    def _store_tokens(self, token_data):
        if not self.handler_storage:
            return
        stored_data = token_data.copy()
        if isinstance(stored_data.get("expires_at"), datetime):
            stored_data["expires_at"] = stored_data["expires_at"].isoformat()
        self.handler_storage.encrypted_json_set("github_tokens", stored_data)

    def _is_token_expired(self, token_data):
        if not token_data or "expires_at" not in token_data:
            return True
        expires_at = token_data["expires_at"]
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        elif isinstance(expires_at, (int, float)):
            expires_at = datetime.fromtimestamp(expires_at, tz=timezone.utc)
        elif not isinstance(expires_at, datetime):
            return True
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        buffer_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        return buffer_time >= expires_at

    def _refresh_tokens(self, refresh_token):
        client_id = self.connection_data.get("client_id")
        client_secret = self.connection_data.get("client_secret")
        if not client_id or not client_secret:
            raise ValueError(
                "client_id and client_secret are required to refresh GitHub OAuth tokens"
            )

        response = requests.post(
            GITHUB_TOKEN_URL,
            headers={"Accept": "application/json"},
            json={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
        )
        response.raise_for_status()
        token_response = response.json()

        if "error" in token_response:
            raise ValueError(
                f"GitHub token refresh failed: {token_response['error']} - "
                f"{token_response.get('error_description', '')}"
            )

        if "refresh_token" not in token_response:
            raise ValueError("GitHub did not return a new refresh_token")

        return {
            "access_token": token_response["access_token"],
            "refresh_token": token_response["refresh_token"],
            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=token_response.get("expires_in", 28800)),
        }

    def _connect_with_oauth(self):
        # Prefer stored tokens (they rotate on each refresh)
        token_data = self._load_stored_tokens()

        if not token_data:
            # First connection: use tokens from connection_data
            access_token = self.connection_data.get("access_token")
            refresh_token = self.connection_data.get("refresh_token")
            if not access_token:
                raise ValueError("access_token is required for OAuth authentication")
            token_data = {
                "access_token": access_token,
                "refresh_token": refresh_token,
            }
            self._store_tokens(token_data)

        # Refresh if expired
        if self._is_token_expired(token_data) and token_data.get("refresh_token"):
            client_id = self.connection_data.get("client_id")
            client_secret = self.connection_data.get("client_secret")
            if client_id and client_secret:
                with self._refresh_lock:
                    # Double-check: another thread may have already refreshed
                    stored = self._load_stored_tokens()
                    if stored and not self._is_token_expired(stored):
                        token_data = stored
                    else:
                        token_data = self._refresh_tokens(token_data["refresh_token"])
                        self._store_tokens(token_data)
            else:
                logger.warning(
                    "OAuth token may be expired but client_id/client_secret not provided for refresh"
                )

        return token_data["access_token"]

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

        if self._uses_oauth():
            access_token = self._connect_with_oauth()
            connection_kwargs["login_or_token"] = access_token
        elif self.connection_data.get("api_key"):
            connection_kwargs["login_or_token"] = self.connection_data["api_key"]

        if self.connection_data.get("github_url"):
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
            assert self.connection is not None
            if self._uses_oauth() or self.connection_data.get("api_key"):
                current_user = self.connection.get_user().name
                logger.info(f"Authenticated as user {current_user}")
            else:
                logger.info("Proceeding without an API key")

            current_limit = self.connection.get_rate_limit()
            logger.info(f"Current rate limit: {current_limit}")
            response.success = True

            if self._uses_oauth():
                response.copy_storage = "success"
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
        ast = parse_sql(query)
        return self.query(ast)
