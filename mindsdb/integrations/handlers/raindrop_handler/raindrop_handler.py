import requests
import time
from typing import Dict, Any, List

from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.raindrop_handler.raindrop_tables import (
    RaindropsTable,
    CollectionsTable,
    TagsTable,
    ParseTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class RaindropHandler(APIHandler):
    """The Raindrop.io handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the Raindrop.io handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        # Register tables
        self._register_table("raindrops", RaindropsTable(self))
        self._register_table("bookmarks", RaindropsTable(self))  # Alias for raindrops
        self._register_table("collections", CollectionsTable(self))
        self._register_table("tags", TagsTable(self))
        self._register_table("parse", ParseTable(self))

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection

        api_key = self.connection_data.get("api_key")
        if not api_key:
            raise ValueError("API key is required for Raindrop.io connection")

        self.connection = RaindropAPIClient(api_key)
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
            # Test the connection by getting user stats
            test_response = self.connection.get_user_stats()
            if test_response.get("result"):
                logger.info("Successfully connected to Raindrop.io API")
                response.success = True
            else:
                logger.error("Failed to connect to Raindrop.io API")
                response.error_message = "Invalid API response"
        except Exception as e:
            logger.error(f"Error connecting to Raindrop.io API: {e}!")
            response.error_message = str(e)

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


class RaindropAPIClient:
    """A client for the Raindrop.io API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.raindrop.io/rest/v1"
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        # Rate limiting: 120 requests per minute = 2 requests per second
        self.rate_limit_per_second = 2
        self.request_times = []

    def _apply_rate_limit(self):
        """Apply rate limiting to avoid hitting API limits"""
        current_time = time.time()

        # Remove requests older than 1 second
        self.request_times = [t for t in self.request_times if current_time - t < 1.0]

        # Check if we need to wait
        if len(self.request_times) >= self.rate_limit_per_second:
            # Calculate how long to wait
            oldest_request = min(self.request_times)
            wait_time = 1.0 - (current_time - oldest_request)

            if wait_time > 0:
                logger.debug(".2f")
                time.sleep(wait_time)
                # Update current_time after sleep
                current_time = time.time()
                # Clean up old requests again after sleep
                self.request_times = [t for t in self.request_times if current_time - t < 1.0]

        # Record this request
        self.request_times.append(current_time)

    def _make_request(
        self, method: str, endpoint: str, params: Dict[str, Any] = None, data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Make a request to the Raindrop.io API with rate limiting"""
        # Apply rate limiting
        self._apply_rate_limit()

        # Validate endpoint to prevent path traversal/injection attacks
        allowed_endpoints = [
            "/user/stats",
            "/raindrops",
            "/raindrop",
            "/collections",
            "/collection",
            "/filters",
            "/tags",
            "/parse",
        ]

        # Normalize endpoint by ensuring it starts with /
        normalized_endpoint = f"/{endpoint.lstrip('/')}"

        # Check if endpoint matches any allowed prefix
        if not any(normalized_endpoint.startswith(prefix) for prefix in allowed_endpoints):
            raise ValueError(f"Invalid endpoint: {endpoint}. Only Raindrop.io API endpoints are allowed.")

        url = f"{self.base_url}{normalized_endpoint}"

        response = requests.request(method=method, url=url, headers=self.headers, params=params, json=data)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            try:
                error_data = response.json()
                error_message = error_data.get("error", error_data.get("message", str(e)))
            except (ValueError, KeyError):
                error_message = str(e)
            raise Exception(f"Raindrop API error: {error_message}")
        return response.json()

    def get_user_stats(self) -> Dict[str, Any]:
        """Get user statistics"""
        return self._make_request("GET", "/user/stats")

    # Raindrops (Bookmarks) methods
    def get_raindrops(
        self,
        collection_id: int = 0,
        search: str = None,
        sort: str = None,
        page: int = 0,
        per_page: int = 50,
        max_results: int = None,
    ) -> Dict[str, Any]:
        """Get raindrops from a collection with optimized pagination"""
        all_items = []
        current_page = page

        # Optimize page size based on max_results to minimize API calls
        if max_results and max_results <= 10:
            # For small limits, use smaller page sizes to avoid wasting requests
            per_page_limit = max(5, min(per_page, max_results))
        elif max_results and max_results <= 25:
            per_page_limit = max(10, min(per_page, max_results))
        else:
            per_page_limit = min(per_page, 50)  # API limit is 50

        while True:
            params = {"page": current_page, "perpage": per_page_limit}

            if search:
                params["search"] = search
            if sort:
                params["sort"] = sort

            response = self._make_request("GET", f"/raindrops/{collection_id}", params=params)

            if not response.get("result", False):
                break

            items = response.get("items", [])
            if not items:
                break

            all_items.extend(items)

            # Check if we've reached max_results limit
            if max_results and len(all_items) >= max_results:
                all_items = all_items[:max_results]
                break

            # Check if we got fewer items than requested (last page)
            if len(items) < per_page_limit:
                break

            current_page += 1

            # Safety check: don't fetch more than 10 pages to prevent infinite loops
            if current_page > 10:
                logger.warning("Stopping pagination after 10 pages to prevent rate limit issues")
                break

        # Return response in same format as original API
        return {"result": True, "items": all_items, "count": len(all_items)}

    def get_raindrop(self, raindrop_id: int) -> Dict[str, Any]:
        """Get a single raindrop"""
        return self._make_request("GET", f"/raindrop/{raindrop_id}")

    def create_raindrop(self, raindrop_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new raindrop"""
        return self._make_request("POST", "/raindrop", data=raindrop_data)

    def update_raindrop(self, raindrop_id: int, raindrop_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing raindrop"""
        return self._make_request("PUT", f"/raindrop/{raindrop_id}", data=raindrop_data)

    def delete_raindrop(self, raindrop_id: int) -> Dict[str, Any]:
        """Delete a raindrop"""
        return self._make_request("DELETE", f"/raindrop/{raindrop_id}")

    def create_multiple_raindrops(self, raindrops_data: list) -> Dict[str, Any]:
        """Create multiple raindrops"""
        return self._make_request("POST", "/raindrops", data={"items": raindrops_data})

    def update_multiple_raindrops(
        self, collection_id: int, update_data: Dict[str, Any], search: str = None, ids: list = None
    ) -> Dict[str, Any]:
        """Update multiple raindrops"""
        data = update_data.copy()
        if search:
            data["search"] = search
        if ids:
            data["ids"] = ids
        return self._make_request("PUT", f"/raindrops/{collection_id}", data=data)

    def delete_multiple_raindrops(self, collection_id: int, search: str = None, ids: list = None) -> Dict[str, Any]:
        """Delete multiple raindrops"""
        data = {}
        if search:
            data["search"] = search
        if ids:
            data["ids"] = ids
        return self._make_request("DELETE", f"/raindrops/{collection_id}", data=data)

    # Collections methods
    def get_collections(self) -> Dict[str, Any]:
        """Get root collections"""
        return self._make_request("GET", "/collections")

    def get_child_collections(self) -> Dict[str, Any]:
        """Get child collections"""
        return self._make_request("GET", "/collections/childrens")

    def get_collection(self, collection_id: int) -> Dict[str, Any]:
        """Get a single collection"""
        return self._make_request("GET", f"/collection/{collection_id}")

    def create_collection(self, collection_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new collection"""
        return self._make_request("POST", "/collection", data=collection_data)

    def update_collection(self, collection_id: int, collection_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing collection"""
        return self._make_request("PUT", f"/collection/{collection_id}", data=collection_data)

    def delete_collection(self, collection_id: int) -> Dict[str, Any]:
        """Delete a collection"""
        return self._make_request("DELETE", f"/collection/{collection_id}")

    def delete_multiple_collections(self, collection_ids: list) -> Dict[str, Any]:
        """Delete multiple collections"""
        return self._make_request("DELETE", "/collections", data={"ids": collection_ids})

    # Advanced filtering methods
    def get_raindrops_with_filters(self, collection_id: int = 0, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get raindrops using advanced filters endpoint"""
        endpoint = f"/filters/{collection_id}"
        return self._make_request("POST", endpoint, data=filters or {})

    def get_tags(self) -> Dict[str, Any]:
        """Get all tags with usage statistics"""
        return self._make_request("GET", "/tags")

    def parse_url(self, url: str) -> Dict[str, Any]:
        """Parse URL to extract metadata"""
        return self._make_request("POST", "/parse", data={"url": url})

    def search_raindrops_advanced(
        self,
        collection_id: int = 0,
        search: str = None,
        tags: List[str] = None,
        important: bool = None,
        sort: str = None,
        page: int = 0,
        per_page: int = 50,
    ) -> Dict[str, Any]:
        """Advanced search with multiple filter criteria"""
        filters = {}

        if search:
            filters["search"] = search
        if tags:
            filters["tags"] = tags
        if important is not None:
            filters["important"] = important
        if sort:
            filters["sort"] = sort

        # Add pagination parameters to filters if provided
        if page is not None:
            filters["page"] = page
        if per_page is not None:
            filters["perpage"] = per_page

        response = self.get_raindrops_with_filters(collection_id, filters)
        return response
