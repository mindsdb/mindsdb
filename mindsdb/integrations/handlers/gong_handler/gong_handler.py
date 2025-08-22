import requests
from typing import Any, Dict

from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.gong_handler.gong_tables import (
    GongCallsTable,
    GongUsersTable,
    GongAnalyticsTable,
    GongTranscriptsTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class GongHandler(APIHandler):
    """
    This handler handles the connection and execution of SQL statements on Gong.
    """

    name = "gong"

    def __init__(self, name: str, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Gong API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.base_url = connection_data.get("base_url", "https://api.gong.io")

        # Support both bearer token and access key + secret key
        self.bearer_token = connection_data.get("api_key")
        self.access_key = connection_data.get("access_key")
        self.secret_key = connection_data.get("secret_key")

        # Register core tables
        self._register_table("calls", GongCallsTable(self))
        self._register_table("users", GongUsersTable(self))
        self._register_table("analytics", GongAnalyticsTable(self))
        self._register_table("transcripts", GongTranscriptsTable(self))

    def connect(self) -> requests.Session:
        """
        Establishes a connection to the Gong API.

        Raises:
            ValueError: If the required connection parameters are not provided.
            Exception: If a connection error occurs.

        Returns:
            requests.Session: A session object for making API requests.
        """
        if self.is_connected is True:
            return self.connection

        if self.access_key and self.secret_key:
            auth_method = "basic"
        elif self.bearer_token:
            auth_method = "bearer"
        else:
            raise ValueError("Either bearer_token or (access_key + secret_key) is required to connect to Gong API.")

        try:
            self.connection = requests.Session()

            if auth_method == "basic":
                # Basic authentication with access key + secret key
                self.connection.auth = (self.access_key, self.secret_key)
                self.connection.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
            else:
                # Bearer token authentication
                self.connection.headers.update(
                    {
                        "Authorization": f"Bearer {self.bearer_token}",
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    }
                )

            test_response = self.connection.get(f"{self.base_url}/v2/users")
            test_response.raise_for_status()

            self.is_connected = True
            return self.connection

        except Exception as e:
            self.is_connected = False
            logger.error(f"Error connecting to Gong API: {e}")
            raise

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Gong API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            self.connect()
            # Test the connection by making a simple API call
            test_response = self.connection.get(f"{self.base_url}/v2/users")
            test_response.raise_for_status()
            response.success = True
        except Exception as e:
            logger.error(f"Connection check to Gong failed: {e}")
            response.error_message = str(e)

        self.is_connected = response.success
        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a native query on Gong and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        try:
            ast = parse_sql(query)
            return self.query(ast)
        except Exception as e:
            logger.error(f"Error running query: {query} on Gong: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_code=0, error_message=str(e))

    def call_gong_api(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Makes a call to the Gong API.

        Args:
            endpoint (str): The API endpoint to call.
            params (Dict): Query parameters for the API call.

        Returns:
            Dict: The API response.
        """
        if not self.is_connected:
            self.connect()

        url = f"{self.base_url}{endpoint}"
        response = self.connection.get(url, params=params)
        response.raise_for_status()
        return response.json()
