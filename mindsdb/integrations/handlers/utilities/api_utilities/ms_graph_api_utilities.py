import time
import requests
from typing import Optional, Dict, Union, List

from mindsdb.integrations.handlers.utilities.auth_utilities.ms_graph_api_auth_utilities import MSGraphAPIAuthManager


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
        ms_graph_auth_manager = MSGraphAPIAuthManager(client_id, client_secret, tenant_id, refresh_token)
        self.access_token = ms_graph_auth_manager.get_access_token()
        self._group_ids = None

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

    def _get_group_ids(self):
        if not self._group_ids:
            api_url = self._get_api_url("groups")
            params = {"$select": "id,resourceProvisioningOptions"}
            groups = self._get_response_value_unsafe(self._make_request(api_url, params=params))
            self._group_ids = [item["id"] for item in groups if "Team" in item["resourceProvisioningOptions"]]
        return self._group_ids

    def get_channels(self):
        channels = []
        for group_id in self._get_group_ids():
            for group_channels in self._fetch_data(f"teams/{group_id}/channels", pagination=False):
                channels.extend(group_channels)

        return channels

    def _get_channel_ids(self, group_id: str):
        api_url = self._get_api_url(f"teams/{group_id}/channels")
        channels_ids = self._get_response_value_unsafe(self._make_request(api_url))
        return channels_ids

    def get_channel_messages(self):
        channel_messages = []
        for group_id in self._get_group_ids():
            for channel_id in self._get_channel_ids(group_id):
                for messages in self._fetch_data(f"teams/{group_id}/channels/{channel_id['id']}/messages"):
                    channel_messages.extend(messages)

        return channel_messages
    