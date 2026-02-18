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

    def get_spaces(
        self,
        ids: List[int] = None,
        keys: List[str] = None,
        space_type: str = None,
        status: str = None,
        sort_condition: str = None,
        limit: int = None,
    ):
        url = f"{self.url}/wiki/api/v2/spaces"
        params = {
            "description-format": "view",
        }
        if ids:
            params["ids"] = ids
        if keys:
            params["keys"] = keys
        if space_type:
            params["type"] = space_type
        if status:
            params["status"] = status
        if sort_condition:
            params["sort"] = sort_condition
        if limit:
            params["limit"] = limit

        return self._paginate(url, params)

    def get_pages(
        self,
        page_ids: List[int] = None,
        space_ids: List[int] = None,
        statuses: List[str] = None,
        title: str = None,
        sort_condition: str = None,
        limit: int = None,
    ) -> List[dict]:
        url = f"{self.url}/wiki/api/v2/pages"
        params = {
            "body-format": "storage",
        }
        if page_ids:
            params["id"] = page_ids
        if space_ids:
            params["space-id"] = space_ids
        if statuses:
            params["status"] = statuses
        if title:
            params["title"] = title
        if sort_condition:
            params["sort"] = sort_condition
        if limit:
            params["limit"] = limit

        return self._paginate(url, params)

    def get_blogposts(
        self,
        post_ids: List[int] = None,
        space_ids: List[str] = None,
        statuses: List[str] = None,
        title: str = None,
        sort_condition: str = None,
        limit: int = None,
    ) -> List[dict]:
        url = f"{self.url}/wiki/api/v2/blogposts"
        params = {
            "body-format": "storage",
        }
        if post_ids:
            params["id"] = post_ids
        if space_ids:
            params["space-id"] = space_ids
        if statuses:
            params["status"] = statuses
        if title:
            params["title"] = title
        if sort_condition:
            params["sort"] = sort_condition
        if limit:
            params["limit"] = limit

        return self._paginate(url, params)

    def get_whiteboard_by_id(self, whiteboard_id: int) -> dict:
        url = f"{self.url}/wiki/api/v2/whiteboards/{whiteboard_id}"

        return self._make_request("GET", url)

    def get_database_by_id(self, database_id: int) -> dict:
        url = f"{self.url}/wiki/api/v2/databases/{database_id}"

        return self._make_request("GET", url)

    def get_tasks(
        self,
        task_ids: List[int] = None,
        space_ids: List[str] = None,
        page_ids: List[str] = None,
        blogpost_ids: List[str] = None,
        created_by_ids: List[str] = None,
        assigned_to_ids: List[str] = None,
        completed_by_ids: List[str] = None,
        status: str = None,
        limit: int = None,
    ) -> List[dict]:
        url = f"{self.url}/wiki/api/v2/tasks"
        params = {
            "body-format": "storage",
        }
        if task_ids:
            params["id"] = task_ids
        if space_ids:
            params["space-id"] = space_ids
        if page_ids:
            params["page-id"] = page_ids
        if blogpost_ids:
            params["blogpost-id"] = blogpost_ids
        if created_by_ids:
            params["created-by"] = created_by_ids
        if assigned_to_ids:
            params["assigned-to"] = assigned_to_ids
        if completed_by_ids:
            params["completed-by"] = completed_by_ids
        if status:
            params["status"] = status
        if limit:
            params["limit"] = limit

        return self._paginate(url, params)

    def _paginate(self, url: str, params: dict = None) -> List[dict]:
        results = []
        response = self._make_request("GET", url, params)
        results.extend(response["results"])

        while response["_links"].get("next"):
            next_url = response["_links"].get("next")
            next_params = {}
            if params:
                next_params.update(params)
            if "cursor=" in next_url:
                # cursor= is 7 characters long
                cursor_start = next_url.find("cursor=") + 7
                cursor_value = next_url[cursor_start:]
                if "&" in cursor_value:
                    cursor_value = cursor_value.split("&")[0]
                next_params["cursor"] = cursor_value
                response = self._make_request("GET", url, next_params)
            else:
                response = self._make_request("GET", next_url)
            results.extend(response["results"])

        return results

    def _make_request(self, method: str, url: str, params: dict = None, data: dict = None) -> dict:
        response = self.session.request(method, url, params=params, json=data)

        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

        return response.json()
