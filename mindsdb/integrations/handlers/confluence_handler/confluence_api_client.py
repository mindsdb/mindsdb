from typing import List, Optional

import requests


class ConfluenceAPIClient:
    def __init__(
        self,
        url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        auth_method: Optional[str] = None,
        is_selfHosted: bool = False,
    ):
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.token = token
        self.auth_method = auth_method
        self.is_selfHosted = is_selfHosted
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

        use_bearer = (auth_method == "bearer") or bool(token)
        if use_bearer:
            if not token:
                raise ValueError("Token must be provided for bearer authentication.")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            if not username or not password:
                raise ValueError("Username and password must be provided for basic authentication.")
            self.session.auth = (username, password)

    def get_spaces(
        self,
        ids: List[int] = None,
        keys: List[str] = None,
        space_type: str = None,
        status: str = None,
        sort_condition: str = None,
        limit: int = None,
    ):
        if self.is_selfHosted:
            return self._get_spaces_server(ids, keys, space_type, status, limit)
        return self._get_spaces_cloud(ids, keys, space_type, status, sort_condition, limit)

    def _get_spaces_cloud(
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

    def _get_spaces_server(
        self,
        ids: List[int] = None,
        keys: List[str] = None,
        space_type: str = None,
        status: str = None,
        limit: int = None,
    ):
        """Fetch spaces via Confluence Server REST API v1."""
        url = f"{self.url}/rest/api/space"
        # expand=description.view to get the description body;
        # expand=homepage to get the homepage page ID.
        params = {
            "expand": "description.view,homepage",
        }
        if ids:
            params["spaceId"] = ids
        if keys:
            params["spaceKey"] = keys
        if space_type:
            params["type"] = space_type
        if status:
            params["status"] = status
        if limit:
            params["limit"] = limit

        results = self._paginate(url, params)
        return [self._normalize_v1_space(s) for s in results]

    def _normalize_v1_space(self, space: dict) -> dict:
        """Normalize a Confluence Server v1 space response to match the Cloud v2 shape."""
        description_view = space.get("description", {}).get("view", {})
        homepage = space.get("homepage", {})

        return {
            "id": space.get("id"),
            "key": space.get("key"),
            "name": space.get("name"),
            "type": space.get("type"),
            "description": {
                "view": {
                    "representation": description_view.get("representation"),
                    "value": description_view.get("value"),
                }
            },
            "status": space.get("status"),
            # Cloud-only fields: set to None so pd.json_normalize produces the
            # expected columns and the DataFrame projection doesn't crash.
            "authorId": None,
            "createdAt": None,
            "homepageId": homepage.get("id"),
            "currentActiveAlias": None,
            "_links": {
                "webui": space.get("_links", {}).get("webui"),
            },
        }

    def get_pages(
        self,
        page_ids: List[int] = None,
        space_ids: List[int] = None,
        statuses: List[str] = None,
        title: str = None,
        sort_condition: str = None,
        limit: int = None,
    ) -> List[dict]:
        if self.is_selfHosted:
            return self._get_pages_server(page_ids, space_ids, statuses, title, limit)
        return self._get_pages_cloud(page_ids, space_ids, statuses, title, sort_condition, limit)

    def _get_pages_cloud(
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

    def _get_pages_server(
        self,
        page_ids: List[int] = None,
        space_ids: List[int] = None,
        statuses: List[str] = None,
        title: str = None,
        limit: int = None,
    ) -> List[dict]:
        """Fetch pages via Confluence Server REST API v1 using CQL search."""
        cql_parts = ["type=page"]
        if page_ids:
            id_cql = " OR ".join([f"id={pid}" for pid in page_ids])
            cql_parts.append(f"({id_cql})")
        if space_ids:
            space_cql = " OR ".join([f"space.id={sid}" for sid in space_ids])
            cql_parts.append(f"({space_cql})")
        if statuses:
            status_cql = " OR ".join([f'status="{s}"' for s in statuses])
            cql_parts.append(f"({status_cql})")
        if title:
            escaped_title = title.replace('"', '\\"')
            cql_parts.append(f'title="{escaped_title}"')

        url = f"{self.url}/rest/api/content/search"
        params = {
            "cql": " AND ".join(cql_parts),
            "expand": "body.storage,version,space,history,ancestors",
        }
        if limit:
            params["limit"] = limit

        results = self._paginate(url, params)
        return [self._normalize_v1_page(p) for p in results]

    def _normalize_v1_page(self, page: dict) -> dict:
        """Normalize a Confluence Server v1 page response to match the Cloud v2 shape."""
        ancestors = page.get("ancestors", [])
        parent = ancestors[-1] if ancestors else {}
        history = page.get("history", {})
        created_by = history.get("createdBy", {})
        version = page.get("version", {})
        version_by = version.get("by", {})

        return {
            "id": page.get("id"),
            "status": page.get("status"),
            "title": page.get("title"),
            "spaceId": page.get("space", {}).get("id"),
            "parentId": parent.get("id"),
            "parentType": parent.get("type"),
            "position": None,
            "authorId": created_by.get("accountId") or created_by.get("username"),
            "ownerId": created_by.get("accountId") or created_by.get("username"),
            "lastOwnerId": None,
            "createdAt": history.get("createdDate"),
            "version": {
                "createdAt": version.get("when"),
                "message": version.get("message", ""),
                "number": version.get("number"),
                "minorEdit": version.get("minorEdit", False),
                "authorId": version_by.get("accountId") or version_by.get("username"),
            },
            "body": page.get("body", {}),
            "_links": {
                "webui": page.get("_links", {}).get("webui"),
                "editui": page.get("_links", {}).get("editui"),
                "tinyui": page.get("_links", {}).get("tinyui"),
            },
        }

    def get_blogposts(
        self,
        post_ids: List[int] = None,
        space_ids: List[str] = None,
        statuses: List[str] = None,
        title: str = None,
        sort_condition: str = None,
        limit: int = None,
    ) -> List[dict]:
        if self.is_selfHosted:
            return self._get_blogposts_server(post_ids, space_ids, statuses, title, limit)
        return self._get_blogposts_cloud(post_ids, space_ids, statuses, title, sort_condition, limit)

    def _get_blogposts_cloud(
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

    def _get_blogposts_server(
        self,
        post_ids: List[int] = None,
        space_ids: List[str] = None,
        statuses: List[str] = None,
        title: str = None,
        limit: int = None,
    ) -> List[dict]:
        """Fetch blog posts via Confluence Server REST API v1 using CQL search."""
        cql_parts = ["type=blogpost"]
        if post_ids:
            id_cql = " OR ".join([f"id={pid}" for pid in post_ids])
            cql_parts.append(f"({id_cql})")
        if space_ids:
            space_cql = " OR ".join([f"space.id={sid}" for sid in space_ids])
            cql_parts.append(f"({space_cql})")
        if statuses:
            status_cql = " OR ".join([f'status="{s}"' for s in statuses])
            cql_parts.append(f"({status_cql})")
        if title:
            escaped_title = title.replace('"', '\\"')
            cql_parts.append(f'title="{escaped_title}"')

        url = f"{self.url}/rest/api/content/search"
        params = {
            "cql": " AND ".join(cql_parts),
            "expand": "body.storage,version,space,history",
        }
        if limit:
            params["limit"] = limit

        results = self._paginate(url, params)
        return [self._normalize_v1_blogpost(p) for p in results]

    def _normalize_v1_blogpost(self, post: dict) -> dict:
        """Normalize a Confluence Server v1 blogpost response to match the Cloud v2 shape."""
        history = post.get("history", {})
        created_by = history.get("createdBy", {})
        version = post.get("version", {})
        version_by = version.get("by", {})

        return {
            "id": post.get("id"),
            "status": post.get("status"),
            "title": post.get("title"),
            "spaceId": post.get("space", {}).get("id"),
            "authorId": created_by.get("accountId") or created_by.get("username"),
            "createdAt": history.get("createdDate"),
            "version": {
                "createdAt": version.get("when"),
                "message": version.get("message", ""),
                "number": version.get("number"),
                "minorEdit": version.get("minorEdit", False),
                "authorId": version_by.get("accountId") or version_by.get("username"),
            },
            "body": post.get("body", {}),
            "_links": {
                "webui": post.get("_links", {}).get("webui"),
                "editui": post.get("_links", {}).get("editui"),
                "tinyui": post.get("_links", {}).get("tinyui"),
            },
        }

    def get_whiteboard_by_id(self, whiteboard_id: int) -> dict:
        if self.is_selfHosted:
            raise NotImplementedError("Whiteboards are only available on Confluence Cloud.")
        url = f"{self.url}/wiki/api/v2/whiteboards/{whiteboard_id}"

        return self._make_request("GET", url)

    def get_database_by_id(self, database_id: int) -> dict:
        if self.is_selfHosted:
            raise NotImplementedError("Databases are only available on Confluence Cloud.")
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
        if self.is_selfHosted:
            raise NotImplementedError(
                "The tasks endpoint is only available on Confluence Cloud. "
                "Confluence Server/Data Center does not expose a dedicated REST API for inline tasks."
            )
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

        while response.get("_links", {}).get("next"):
            next_path = response["_links"]["next"]
            if "cursor=" in next_path:
                # Cloud v2 cursor-based pagination: reconstruct request against the
                # original URL with the cursor value appended as a query param.
                next_params = {}
                if params:
                    next_params.update(params)
                cursor_start = next_path.find("cursor=") + 7
                cursor_value = next_path[cursor_start:]
                if "&" in cursor_value:
                    cursor_value = cursor_value.split("&")[0]
                next_params["cursor"] = cursor_value
                response = self._make_request("GET", url, next_params)
            else:
                # Offset-based pagination (Cloud v1 / Server): next_path may be
                # relative (e.g. "/rest/api/space?start=25&limit=25").  Reconstruct
                # the absolute URL using _links.base when present.
                if next_path.startswith("http"):
                    next_url = next_path
                else:
                    base = response["_links"].get("base", self.url)
                    next_url = base.rstrip("/") + next_path
                response = self._make_request("GET", next_url)
            results.extend(response["results"])

        return results

    def _make_request(self, method: str, url: str, params: dict = None, data: dict = None) -> dict:
        response = self.session.request(method, url, params=params, json=data)

        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

        return response.json()
