import requests
import time
from typing import Dict, Generator, List, Optional, Text, Union

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSGraphAPIBaseClient:
    """
    The base class for the Microsoft Graph API clients.
    This class contains common methods for accessing the Microsoft Graph API.

    Attributes:
        MICROSOFT_GRAPH_BASE_API_URL (Text): The base URL of the Microsoft Graph API.
        MICROSOFT_GRAPH_API_VERSION (Text): The version of the Microsoft Graph API.
        PAGINATION_COUNT (Optional[int]): The number of items to retrieve per request.
    """
    MICROSOFT_GRAPH_BASE_API_URL: Text = "https://graph.microsoft.com/"
    MICROSOFT_GRAPH_API_VERSION: Text = "v1.0"
    PAGINATION_COUNT: Optional[int] = 20

    def __init__(self, access_token: Text) -> None:
        """
        Initializes the Microsoft Graph API client.

        Args:
            access_token (Text): The access token for authenticating the requests to the Microsoft Graph API.
        """
        self.access_token = access_token
        self._group_ids = None

    def _get_api_url(self, endpoint: Text) -> Text:
        """
        Constructs the API URL for the specified endpoint.

        Args:
            endpoint (Text): The endpoint of the Microsoft Graph API.

        Returns:
            Text: The fully constructed API URL.
        """
        api_url = f"{self.MICROSOFT_GRAPH_BASE_API_URL}{self.MICROSOFT_GRAPH_API_VERSION}/{endpoint}/"
        return api_url

    def _make_request(
        self,
        api_url: Text,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        method: Text = "GET"
    ) -> Union[Dict, object]:
        """
        Makes a request to the Microsoft Graph API.

        Args:
            api_url (Text): The API URL to make the request to.
            params (Optional[Dict]): The parameters to include in the request.
            data (Optional[Dict]): The data to include in the request.
            method (Text): The HTTP method to use for the request.

        Returns:
            Union[Dict, object]: The response content of the request.
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}

        # Make the request to the Microsoft Graph API based on the method.
        if method == "GET":
            response = requests.get(api_url, headers=headers, params=params)
        elif method == "POST":
            response = requests.post(api_url, headers=headers, json=data)
        else:
            raise NotImplementedError(f"Method {method} not implemented")

        # Process the response.
        # If the response is a 429 (rate limit exceeded), wait for the specified time and retry.
        if response.status_code == 429:
            if "Retry-After" in response.headers:
                pause_time = float(response.headers["Retry-After"])
                time.sleep(pause_time)
                response = requests.get(api_url, headers=headers, params=params)

        # If the response is not successful, raise an exception.
        if response.status_code not in [200, 201]:
            raise requests.exceptions.RequestException(response.text)

        return response

    def fetch_paginated_data(self, endpoint: Text, params: Optional[Dict] = None) -> Generator:
        """
        Fetches data from the Microsoft Graph API by making the specified request and handling pagination.

        Args:
            endpoint (str): The endpoint of the Microsoft Graph API to fetch data from.
            params (Optional[Dict]): The parameters to include in the request.

        Yields:
            List: The data fetched from the Microsoft Graph API.
        """
        if params is None:
            params = {}
        api_url = self._get_api_url(endpoint)

        # Add the pagination count to the request parameters.
        if "$top" not in params:
            params["$top"] = self.PAGINATION_COUNT

        while api_url:
            # Make the initial request to the Microsoft Graph API.
            response = self._make_request(api_url, params)
            response_json = response.json()
            value = response.json()["value"]

            # Get the next page of data if pagination is enabled.
            params = None
            api_url = response_json.get("@odata.nextLink", "")
            yield value

    def _fetch_data(self, endpoint: str, params: Optional[Dict] = {}) -> Union[List, Dict, bytes]:
        """
        Fetches data from the Microsoft Graph API by making the specified request.

        Args:
            endpoint (str): The endpoint of the Microsoft Graph API to fetch data from.
            params (Optional[Dict]): The parameters to include in the request.

        Returns:
            Union[List, Dict, bytes]: The data fetched from the Microsoft Graph API.
        """
        api_url = self._get_api_url(endpoint)

        response = self._make_request(api_url, params)
        return response

    def fetch_data_content(self, endpoint: str, params: Optional[Dict] = {}) -> bytes:
        """
        Fetches data content from the Microsoft Graph API by making the specified request.

        Args:
            endpoint (str): The endpoint of the Microsoft Graph API to fetch data from.
            params (Optional[Dict]): The parameters to include in the request.

        Returns:
            bytes: The data content fetched from the Microsoft Graph API.
        """
        response = self._fetch_data(endpoint, params)
        return response.content

    def fetch_data_json(self, endpoint: str, params: Optional[Dict] = {}) -> Union[List, Dict]:
        """
        Fetches data from the Microsoft Graph API by making the specified request and returns the JSON response.

        Args:
            endpoint (str): The endpoint of the Microsoft Graph API to fetch data from.
            params (Optional[Dict]): The parameters to include in the request.

        Returns:
            Union[List, Dict]: The JSON response fetched from the Microsoft Graph API.
        """
        response = self._fetch_data(endpoint, params)
        response_json = response.json()

        if "value" in response_json:
            return response_json["value"]
        return response_json
