import requests
from datetime import datetime, timezone
from typing import Text, List, Dict, Any

from requests import Response


class SharepointAPI:
    def __init__(
        self, client_id: str = None, client_secret: str = None, tenant_id: str = None
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.bearer_token = None
        self.is_connected = False
        self.expiration_time = datetime.now(timezone.utc).timestamp()

    def get_bearer_token(self) -> None:
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"

        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": "http://localhost",
            "grant_type": "client_credentials",
            "resource": "https://graph.microsoft.com",
        }
        files = []
        headers = {
            "Cookie": "fpc=Ajxxw0Xf-Z1Aq1j2Kvw_bxdNzkviAQAAAITkudwOAAAA; stsservicecookie=estsfd; "
            "x-ms-gateway-slice=estsfd "
        }

        response = self.getresponse(
            request_type="POST", url=url, headers=headers, payload=payload, files=files
        )
        self.bearer_token = response.json()["access_token"]
        self.expiration_time = int(response.json()["expires_on"])
        self.is_connected = True

    @staticmethod
    def getresponse(
        url: str,
        payload: Dict[Text, Any],
        files: List[Any],
        headers: Dict[Text, Any],
        request_type: str,
    ) -> Response:
        response = requests.request(
            request_type, url, headers=headers, data=payload, files=files
        )
        status_code = response.status_code

        if 400 <= status_code <= 499:
            raise Exception("Client error: " + response.text)

        if 500 <= status_code <= 599:
            raise Exception("Server error: " + response.text)
        return response

    def check_connection(self) -> bool:
        if (
            self.is_connected
            and datetime.now(timezone.utc).astimezone().timestamp()
            < self.expiration_time
        ):
            return True
        else:
            return False

    def disconnect(self) -> None:
        self.bearer_token = None
        self.is_connected = False

    def get_all_sites(self, limit: int = None) -> List[Dict[Text, Any]]:
        url = "https://graph.microsoft.com/v1.0/sites?search=*"

        payload = {}
        headers = {"Authorization": f"Bearer {self.bearer_token}"}

        response = self.getresponse(
            request_type="GET", url=url, headers=headers, payload=payload, files=[]
        )
        response = response.json()["value"]
        if limit:
            response = response[:limit]

        return response

    def get_lists_by_site(
        self, site_id: str, limit: int = None
    ) -> List[Dict[Text, Any]]:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        payload = {}
        headers = {"Authorization": f"Bearer {self.bearer_token}"}

        response = self.getresponse(
            request_type="GET", url=url, headers=headers, payload=payload, files=[]
        )
        response = response.json()["value"]
        if limit:
            response = response[:limit]
        return response

    def get_all_lists(self, limit: int = None) -> List[Dict[Text, Any]]:
        sites = self.get_all_sites()
        lists = []
        for site in sites:
            for list_dict in self.get_lists_by_site(site_id=site["id"].split(",")[1]):
                list_dict["siteName"] = site["name"]
                list_dict["siteId"] = site["id"].split(",")[1]
                lists.append(list_dict)
        if limit:
            lists = lists[:limit]
        return lists

    def delete_lists(self, list_dict: List[Dict[Text, Any]]) -> None:
        for list_entry in list_dict:
            self.delete_a_list(site_id=list_entry["siteId"], list_id=list_entry["id"])

    def delete_a_list(self, site_id: str, list_id: str) -> None:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}"
        payload = {}
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="DELETE", url=url, headers=headers, payload=payload, files=[]
        )

    def update_lists(
        self, list_dict: List[Dict[Text, Text]], values_to_update: Dict[Text, Any]
    ) -> None:
        for list_entry in list_dict:
            self.update_a_list(
                site_id=list_entry["siteId"],
                list_id=list_entry["id"],
                values_to_update=values_to_update,
            )

    def update_a_list(
        self, site_id: str, list_id: str, values_to_update: Dict[Text, Any]
    ) -> None:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}"
        payload = values_to_update
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="POST", url=url, headers=headers, payload=payload, files=[]
        )

    def create_lists(self, data: List[Dict[Text, Any]]) -> None:
        for entry in data:
            self.create_a_list(
                site_id=entry["siteId"],
                column=entry["column"],
                display_name=entry["displayName"],
                list_template=entry.get("list"),
            )

    def create_a_list(
        self, site_id: str, list_template: str, display_name: str, column: str = None
    ) -> None:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/"
        payload = {}
        if column:
            column = eval(column)
            payload["column"] = column
        payload["displayName"] = display_name
        payload["list"] = eval(list_template)
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="POST", url=url, headers=headers, payload=payload, files=[]
        )

    def get_sharepoint_columns_by_site(
        self, site_id: str, limit: int = None
    ) -> List[Dict[Text, Any]]:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/columns/"
        payload = {}
        headers = {"Authorization": f"Bearer {self.bearer_token}"}

        response = self.getresponse(
            request_type="GET", url=url, headers=headers, payload=payload, files=[]
        )
        response = response.json()["value"]
        if limit:
            response = response[:limit]
        return response

    def get_all_sharepoint_columns(self, limit: int = None) -> List[Dict[Text, Any]]:
        sites = self.get_all_sites()
        sharepoint_columns = []
        for site in sites:
            for sharepoint_column_dict in self.get_sharepoint_columns_by_site(
                site_id=site["id"].split(",")[1]
            ):
                sharepoint_column_dict["siteName"] = site["name"]
                sharepoint_column_dict["siteId"] = site["id"].split(",")[1]
                sharepoint_columns.append(sharepoint_column_dict)
        if limit:
            sharepoint_columns = sharepoint_columns[:limit]
        return sharepoint_columns

    def update_sharepoint_columns(
        self,
        sharepoint_column_dict: List[Dict[Text, Text]],
        values_to_update: Dict[Text, Any],
    ) -> None:
        for sharepoint_column_entry in sharepoint_column_dict:
            self.update_a_sharepoint_column(
                site_id=sharepoint_column_entry["siteId"],
                column_id=sharepoint_column_entry["id"],
                values_to_update=values_to_update,
            )

    def update_a_sharepoint_column(
        self, site_id: str, column_id: str, values_to_update: Dict[Text, Any]
    ):
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/columns/{column_id}"
        payload = values_to_update
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="POST", url=url, headers=headers, payload=payload, files=[]
        )

    def delete_sharepoint_columns(self, column_dict: List[Dict[Text, Any]]) -> None:
        for column_entry in column_dict:
            self.delete_a_sharepoint_columns(
                site_id=column_entry["siteId"], column_id=column_entry["id"]
            )

    def delete_a_sharepoint_columns(self, site_id: str, column_id: str) -> None:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{column_id}"
        payload = {}
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="DELETE", url=url, headers=headers, payload=payload, files=[]
        )

    def create_sharepoint_columns(self, data: List[Dict[Text, Any]]) -> None:
        for entry in data:
            self.create_a_sharepoint_column(
                site_id=entry["siteId"],
                enforce_unique_values=entry.get("enforceUniqueValues"),
                hidden=entry.get("hidden"),
                indexed=entry.get("indexed"),
                name=entry["name"],
                text=entry.get("text"),
            )

    def create_a_sharepoint_column(
        self,
        site_id: str,
        enforce_unique_values: bool,
        hidden: bool,
        indexed: bool,
        name: str,
        text: str = None,
    ) -> None:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/"
        payload = {}
        if text:
            text = eval(text)
            payload["text"] = text
        payload["name"] = name
        if enforce_unique_values is not None:
            payload["enforceUniqueValues"] = enforce_unique_values
        if hidden is not None:
            payload["hidden"] = hidden
        if indexed is not None:
            payload["indexed"] = indexed
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="POST", url=url, headers=headers, payload=payload, files=[]
        )

    def get_items_by_sites_and_lists(
        self, site_id: str, list_id: str, limit: int = None
    ) -> List[Dict[Text, Any]]:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/"
        payload = {}
        headers = {"Authorization": f"Bearer {self.bearer_token}"}

        response = self.getresponse(
            request_type="GET", url=url, headers=headers, payload=payload, files=[]
        )
        response = response.json()["value"]
        if limit:
            response = response[:limit]
        return response

    def get_all_items(self, limit: int = None) -> List[Dict[Text, Any]]:
        sites = self.get_all_sites()
        items = []
        for site in sites:
            site_id = site["id"].split(",")[1]
            for sharepoint_list in self.get_lists_by_site(site_id=site_id):
                for item_dict in self.get_items_by_sites_and_lists(
                    site_id=site["id"].split(",")[1], list_id=sharepoint_list["id"]
                ):
                    item_dict["siteName"] = site["name"]
                    item_dict["siteId"] = site["id"].split(",")[1]
                    item_dict["listId"] = sharepoint_list["id"]
                    item_dict["list_name"] = sharepoint_list["displayName"]
                    items.append(item_dict)
        if limit:
            items = items[:limit]
        return items

    def update_items(
        self, item_dict: List[Dict[Text, Text]], values_to_update: Dict[Text, Any]
    ) -> None:
        for item_entry in item_dict:
            self.update_an_item(
                site_id=item_entry["siteId"],
                list_id=item_entry["listId"],
                item_id=item_entry["id"],
                values_to_update=values_to_update,
            )

    def update_an_item(
        self,
        site_id: str,
        list_id: str,
        item_id: str,
        values_to_update: Dict[Text, Any],
    ):
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}"
        payload = values_to_update
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="POST", url=url, headers=headers, payload=payload, files=[]
        )

    def delete_items(self, item_dict: List[Dict[Text, Any]]) -> None:
        for item_entry in item_dict:
            self.delete_an_item(
                site_id=item_entry["siteId"],
                list_id=item_entry["listId"],
                item_id=item_entry["id"],
            )

    def delete_an_item(self, site_id: str, list_id: str, item_id: str) -> None:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}"
        payload = {}
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="DELETE", url=url, headers=headers, payload=payload, files=[]
        )

    def create_items(self, data: List[Dict[Text, Any]]) -> None:
        for entry in data:
            self.create_an_item(
                site_id=entry["siteId"],
                list_id=entry["listId"],
                fields=entry.get("fields"),
            )

    def create_an_item(self, site_id: str, list_id: str, fields: str) -> None:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/"
        payload = {}
        if fields:
            payload["fields"] = eval(fields)
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.getresponse(
            request_type="POST", url=url, headers=headers, payload=payload, files=[]
        )
