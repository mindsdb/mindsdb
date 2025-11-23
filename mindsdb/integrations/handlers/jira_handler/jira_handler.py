from typing import Any, Dict

from atlassian import Jira
from requests.exceptions import HTTPError

from mindsdb.integrations.handlers.jira_handler.jira_tables import (
    JiraProjectsTable,
    JiraIssuesTable,
    JiraUsersTable,
    JiraGroupsTable,
)
from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)

DEFAULT_TABLES = ["projects", "issues", "users", "groups", "attachments", "comments"]


class JiraHandler(MetaAPIHandler):
    """
    This handler handles the connection and execution of SQL statements on Jira.
    """

    def __init__(self, name: str, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Jira API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = self._normalize_connection_data(connection_data)
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        for table in self.connection_data["register_tables"]:
            if table == "projects":
                self._register_table("projects", JiraProjectsTable(self))
            elif table == "issues":
                self._register_table("issues", JiraIssuesTable(self))
            elif table == "groups":
                self._register_table("groups", JiraGroupsTable(self))
            elif table == "users":
                self._register_table("users", JiraUsersTable(self))
            elif table == "attachments":
                self._register_table("attachments", JiraAttachmentsTable(self))
            elif table == "comments":
                self._register_table("comments", JiraCommentsTable(self))

    def _normalize_connection_data(self, connection_data: Dict) -> Dict:
        normalized_data: Dict[str, Any] = {}
        if "jira_url" not in connection_data:
            raise ValueError(
                "The 'jira_url' parameter is required in the connection data."
            )
        else:
            normalized_data["url"] = connection_data["jira_url"]

        normalized_data["cloud"] = connection_data.get("cloud", False)

        if normalized_data["cloud"]:
            if (
                "jira_username" not in connection_data
                or "jira_api_token" not in connection_data
            ):
                raise ValueError(
                    "For Jira Cloud, both 'jira_username' and 'jira_api_token' parameters are required in the connection data."
                )
            normalized_data["username"] = connection_data["jira_username"]
            normalized_data["api_token"] = connection_data["jira_api_token"]
        else:
            if "jira_personal_access_token" in connection_data:
                normalized_data["personal_access_token"] = connection_data[
                    "jira_personal_access_token"
                ]
            elif (
                "jira_username" in connection_data
                and "jira_password" in connection_data
            ):
                normalized_data["username"] = connection_data["jira_username"]
                normalized_data["password"] = connection_data["jira_password"]
            else:
                raise ValueError(
                    "For Jira Server, either 'jira_personal_access_token' or both 'jira_username' and 'jira_password' parameters are required in the connection data."
                )
        normalized_data["register_tables"] = connection_data.get(
            "register_tables", DEFAULT_TABLES
        )

        return normalized_data

    def connect(self) -> Jira:
        """
        Establishes a connection to the Jira API.

        Raises:
            ValueError: If the required connection parameters are not provided.
            AuthenticationError: If an authentication error occurs while connecting to the Salesforce API.

        Returns:
            atlassian.jira.Jira: A connection object to the Jira API.
        """
        if self.is_connected is True:
            return self.connection

        is_cloud = self.connection_data.get("cloud", True)

        if is_cloud:
            # Jira Cloud supports API token authentication.
            if not all(
                key in self.connection_data for key in ["username", "api_token", "url"]
            ):
                raise ValueError(
                    "Required parameters (username, api_token, url) must be provided."
                )

            config = {
                "username": self.connection_data["username"],
                "password": self.connection_data["api_token"],
                "url": self.connection_data["url"],
                "cloud": is_cloud,
            }
        else:
            # Jira Server
            if "url" not in self.connection_data:
                raise ValueError("Required parameter 'url' must be provided.")

            config = {"url": self.connection_data["url"], "cloud": False}

            if "personal_access_token" in self.connection_data:
                config["token"] = self.connection_data["personal_access_token"]
            elif (
                "username" in self.connection_data
                and "password" in self.connection_data
            ):
                config["username"] = self.connection_data["username"]
                config["password"] = self.connection_data["password"]

        try:
            self.connection = Jira(**config)
            self.is_connected = True
            return self.connection
        except Exception as unknown_error:
            logger.error(f"Unknown error connecting to Jira, {unknown_error}!")
            raise

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Salesforce API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            connection.myself()
            response.success = True
        except (HTTPError, ValueError) as known_error:
            logger.error(f"Connection check to Jira failed, {known_error}!")
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(
                f"Connection check to Jira failed due to an unknown error, {unknown_error}!"
            )
            response.error_message = str(unknown_error)

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a native JQL query on Jira and returns the result.

        Args:
            query (Text): The JQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        connection = self.connect()

        try:
            results = connection.jql(query)
            df = JiraIssuesTable(self).normalize(results["issues"])
            response = Response(RESPONSE_TYPE.TABLE, df)
        except HTTPError as http_error:
            logger.error(f"Error running query: {query} on Jira, {http_error}!")
            response = Response(
                RESPONSE_TYPE.ERROR, error_code=0, error_message=str(http_error)
            )
        except Exception as unknown_error:
            logger.error(f"Error running query: {query} on Jira, {unknown_error}!")
            response = Response(
                RESPONSE_TYPE.ERROR, error_code=0, error_message=str(unknown_error)
            )

        return response
