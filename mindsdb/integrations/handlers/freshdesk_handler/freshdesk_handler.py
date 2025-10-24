import base64
import requests
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.freshdesk_handler.freshdesk_tables import (
    FreshdeskAgentsTable,
    FreshdeskTicketsTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class FreshdeskHandler(APIHandler):
    """The Freshdesk handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the Freshdesk handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.api_key = connection_data.get("api_key")
        self.domain = connection_data.get("domain")

        # Remove protocol if included and ensure proper format
        if self.domain:
            self.domain = self.domain.replace("https://", "").replace("http://", "")
            if not self.domain.endswith(".freshdesk.com"):
                if "." not in self.domain:
                    self.domain = f"{self.domain}.freshdesk.com"

        self.base_url = f"https://{self.domain}"

        # Use shared session for better performance and connection pooling
        if not hasattr(FreshdeskHandler, '_shared_session'):
            FreshdeskHandler._shared_session = requests.Session()
        self.session = FreshdeskHandler._shared_session
        self.is_connected = False

        # Register tables - focusing on the two main tables you requested
        self._register_table("agents", FreshdeskAgentsTable(self))
        self._register_table("tickets", FreshdeskTicketsTable(self))

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)

        if not self.api_key or not self.domain:
            resp.error_message = "Missing required connection parameters: api_key and domain"
            return resp

        try:
            # Configure authentication for shared session
            # Freshdesk uses basic auth with API key as username and 'X' as password
            auth_string = f"{self.api_key}:X"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            self.session.headers.update({
                "Authorization": f"Basic {encoded_auth}",
                "Content-Type": "application/json"
            })

            # Test connection by making a simple API call
            test_url = f"{self.base_url}/api/v2/agents"
            response = self.session.get(test_url)

            if response.status_code == 200:
                self.is_connected = True
                resp.success = True
            elif response.status_code == 401:
                resp.error_message = "Authentication failed. Please check your API key."
            elif response.status_code == 404:
                resp.error_message = "Domain not found. Please check your Freshdesk domain."
            else:
                resp.error_message = f"Connection failed with status code: {response.status_code}"

        except Exception as ex:
            resp.error_message = f"Connection failed: {str(ex)}"
            self.is_connected = False

        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.
        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = self.connect()
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

    def call_freshdesk_api(self, endpoint: str, method: str = "GET", params: dict = None, paginate: bool = True) -> dict:
        """Make API calls to Freshdesk with pagination support.
        
        Parameters
        ----------
        endpoint : str
            API endpoint (e.g., "/api/v2/tickets")
        method : str
            HTTP method (GET, POST, PUT, DELETE)
        params : dict
            Query parameters or request body
        paginate : bool
            Whether to automatically handle pagination for GET requests
            
        Returns
        -------
        dict or list
            API response data (list for paginated GET requests, dict otherwise)
        """
        # Security: Validate endpoint to prevent path traversal and SSRF
        if not endpoint.startswith("/api/v2/"):
            raise ValueError("Invalid endpoint: must start with /api/v2/")
        if ".." in endpoint or "//" in endpoint:
            raise ValueError("Invalid endpoint: path traversal detected")

        if not self.is_connected:
            self.connect()

        url = f"{self.base_url}{endpoint}"

        try:
            if method.upper() == "GET" and paginate:
                # Handle pagination for GET requests
                all_results = []
                page = 1
                while True:
                    page_params = params.copy() if params else {}
                    page_params["page"] = page
                    page_params["per_page"] = 100  # Freshdesk max per page

                    response = self.session.get(url, params=page_params)
                    response.raise_for_status()

                    data = response.json() if response.content else []
                    if not data:
                        break

                    all_results.extend(data)

                    # If we got less than the max per page, we're done
                    if len(data) < 100:
                        break

                    page += 1

                return all_results

            elif method.upper() == "GET":
                # Non-paginated GET request
                response = self.session.get(url, params=params)
                response.raise_for_status()
                return response.json() if response.content else {}

            elif method.upper() == "POST":
                response = self.session.post(url, json=params)
                response.raise_for_status()
                return response.json() if response.content else {}

            elif method.upper() == "PUT":
                response = self.session.put(url, json=params)
                response.raise_for_status()
                return response.json() if response.content else {}

            elif method.upper() == "DELETE":
                response = self.session.delete(url)
                response.raise_for_status()
                return response.json() if response.content else {}

            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Freshdesk API call failed: {e}")
            raise e