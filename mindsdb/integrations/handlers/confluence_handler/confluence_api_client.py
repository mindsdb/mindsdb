from typing import List

import requests


class ConfluenceAPIClient:
    def __init__(self, url: str, username: str, password: str):
        self.url = url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({"Accept": "application/json"})

    def get_pages(self, limit: int = None) -> List[dict]:
        url = f"{self.url}/wiki/api/v2/pages"
        params = {
            "body-format": "storage",
            "limit": limit if limit else 25,
        }
        return self._paginate(url, params)

    def get_page_by_id(self, page_id: int, limit: int = None) -> dict:
        url = f"{self.url}/wiki/api/v2/pages/{page_id}"
        params = {
            "body-format": "storage",
            "limit": limit if limit else 25,
        }
        response = self._make_request("GET", url, params)
        return response

    def get_pages_in_space(self, space_id: int) -> List[dict]:
        url = f"{self.url}/wiki/api/v2/spaces/{space_id}/pages"
        params = {
            "body-format": "storage"
        }
        return self._paginate(url, params)

    def _paginate(self, url: str, params: dict = None) -> List[dict]:
        results = []
        response = self._make_request("GET", url, params)
        results.extend(response["results"])

        while response["_links"].get("next"):
            params["cursor"] = response["_links"].get("next")
            response = self._make_request("GET", url, params)
            results.extend(response["results"])

        return results

    def _make_request(self, method: str, url: str, params: dict = None, data: dict = None) -> dict:
        response = self.session.request(method, url, params=params, json=data)

        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

        return response.json()