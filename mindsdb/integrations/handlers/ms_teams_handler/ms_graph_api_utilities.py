import time
import requests
from typing import Optional, Dict, Union, List

from .ms_graph_auth_utilities import MSGraphAuthManager


class MSGraphAPIClient:
    MICROSOFT_GRAPH_BASE_API_URL: str = "https://graph.microsoft.com/"
    MICROSOFT_GRAPH_API_VERSION: str = "v1.0"
    PAGINATION_COUNT: Optional[int] = 20

    def __init__(self, client_id: str, client_secret: str, tenant_id: str, refresh_token: str = None):
        """
        Initializes the class with the client_id, client_secret and tenant_id
        :param client_id: The client_id of the app
        :param client_secret: The client_secret of the app
        :param tenant_id: The tenant_id of the app
        :param refresh_token: The refresh_token of the app
        """
        ms_graph_auth_manager = MSGraphAuthManager(client_id, client_secret, tenant_id, refresh_token)
        self.access_token = ms_graph_auth_manager.get_access_token()

    def _get_api_url(self, endpoint: str) -> str:
        api_url = f"{self.MICROSOFT_GRAPH_BASE_API_URL}{self.MICROSOFT_GRAPH_API_VERSION}/{endpoint}/"
        return api_url

    def _make_request(self, api_url: str, params: Optional[Dict] = None) -> Union[Dict, object]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 429:
            if "Retry-After" in response.headers:
                pause_time = float(response.headers["Retry-After"])
                time.sleep(pause_time)
                response = requests.get(api_url, headers=headers, params=params)
        if response.status_code != 200:
            raise requests.exceptions.RequestException(response.text)
        if response.headers["Content-Type"] == "application/octet-stream":
            raw_response = response.content
        else:
            raw_response = response.json()
        return raw_response
    
    def _get_request_params(self, params: Optional[Dict] = None, pagination: bool = True) -> Dict:
        if self.PAGINATION_COUNT and pagination:
            params = params if params else {}
            if "$top" not in params:
                params["$top"] = self.PAGINATION_COUNT
        return params
    
    def _get_response_value_unsafe(raw_response: Dict) -> List:
        value = raw_response["value"]
        return value
    
    def fetch_data(self, endpoint: str, params: Optional[Dict] = None, pagination: bool = True):
        api_url = self._get_api_url(endpoint)
        params = self._get_request_params(params, pagination)
        while api_url:
            raw_response = self._make_request(api_url, params)
            value = self._get_response_value_unsafe(raw_response)
            params = None
            api_url = raw_response.get("@odata.nextLink", "")
            yield value


    