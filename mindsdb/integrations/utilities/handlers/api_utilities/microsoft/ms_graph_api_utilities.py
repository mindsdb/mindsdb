import time
import requests
from typing import Optional, Dict, Union, List

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSGraphAPIBaseClient:
    MICROSOFT_GRAPH_BASE_API_URL: str = "https://graph.microsoft.com/"
    MICROSOFT_GRAPH_API_VERSION: str = "v1.0"
    PAGINATION_COUNT: Optional[int] = 20

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self._group_ids = None

    def _get_api_url(self, endpoint: str) -> str:
        api_url = f"{self.MICROSOFT_GRAPH_BASE_API_URL}{self.MICROSOFT_GRAPH_API_VERSION}/{endpoint}/"
        return api_url

    def _make_request(self, api_url: str, params: Optional[Dict] = None, data: Optional[Dict] = None, method: str = "GET") -> Union[Dict, object]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if method == "GET":
            response = requests.get(api_url, headers=headers, params=params)
        elif method == "POST":
            response = requests.post(api_url, headers=headers, json=data)
        else:
            raise NotImplementedError(f"Method {method} not implemented")
        if response.status_code == 429:
            if "Retry-After" in response.headers:
                pause_time = float(response.headers["Retry-After"])
                time.sleep(pause_time)
                response = requests.get(api_url, headers=headers, params=params)
        if response.status_code not in [200, 201]:
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

    @staticmethod
    def _get_response_value_unsafe(raw_response: Dict) -> List:
        value = raw_response["value"]
        return value

    def _fetch_data(self, endpoint: str, params: Optional[Dict] = None, pagination: bool = True):
        api_url = self._get_api_url(endpoint)
        params = self._get_request_params(params, pagination)
        while api_url:
            raw_response = self._make_request(api_url, params)
            value = self._get_response_value_unsafe(raw_response)
            params = None
            api_url = raw_response.get("@odata.nextLink", "")
            yield value

    def get_user_profile(self):
        api_url = self._get_api_url("me")
        user_profile = self._make_request(api_url)
        return user_profile

    def check_connection(self):
        try:
            self.get_user_profile()
            return True
        except Exception as e:
            logger.error(f'Error connecting to the Microsoft Graph API: {e}!')
            return False
