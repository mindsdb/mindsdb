from __future__ import annotations

from typing import Dict, List, Optional

import requests
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.pocketbase_handler.pocketbase_table import PocketbaseTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class PocketbaseHandler(APIHandler):
    """Handler that exposes PocketBase collections as SQL tables."""

    AUTH_ENDPOINT = "/api/admins/auth-with-password"
    COLLECTIONS_ENDPOINT = "/api/collections"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        connection_data = kwargs.get("connection_data") or {}
        self.base_url = (connection_data.get("url") or "").rstrip("/")
        self.email = connection_data.get("email")
        self.password = connection_data.get("password")
        self.allowed_collections = connection_data.get("collections")
        self.connection = None
        self.session = requests.Session()
        self.token: Optional[str] = None

        if not self.base_url or not self.email or not self.password:
            raise ValueError("PocketBase handler requires 'url', 'email', and 'password' connection arguments.")

    def connect(self) -> StatusResponse:
        """Authenticate and register the available collections."""

        if self.is_connected:
            return StatusResponse(True)

        try:
            if not self.token:
                self._authenticate()

            self._bootstrap_tables()
            self.is_connected = True
            return StatusResponse(True)
        except Exception as exc:
            logger.exception("Failed to connect to PocketBase")
            self.is_connected = False
            return StatusResponse(False, error_message=str(exc))

    def disconnect(self):
        self.session.close()
        self.token = None
        self.connection = None
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)
        try:
            self.connect()
            # Lightweight metadata request to ensure the API is responsive.
            self._request("GET", self.COLLECTIONS_ENDPOINT, params={"page": 1, "perPage": 1})
            response.success = True
        except Exception as exc:
            logger.error("PocketBase connection check failed: %s", exc)
            response.error_message = str(exc)
            self.is_connected = False
        return response

    def native_query(self, query: str):
        ast = parse_sql(query)
        return self.query(ast)

    def _bootstrap_tables(self):
        """Fetch collections and register tables."""

        collections = self._fetch_collections()
        self._tables = {}
        allowed = None
        if isinstance(self.allowed_collections, list) and self.allowed_collections:
            allowed = {name.lower() for name in self.allowed_collections}

        for collection in collections:
            name = collection.get("name")
            if not name:
                continue
            if allowed and name.lower() not in allowed:
                continue
            table = PocketbaseTable(handler=self, collection=collection)
            self._register_table(name, table)

    def _fetch_collections(self) -> List[dict]:
        """Retrieve all PocketBase collections, handling pagination."""

        page = 1
        collections: List[dict] = []
        while True:
            response = self._request(
                "GET",
                self.COLLECTIONS_ENDPOINT,
                params={"page": page, "perPage": 200},
            )
            items = response.get("items", [])
            collections.extend(items)

            total_items = response.get("totalItems")
            per_page = response.get("perPage") or len(items) or 1
            total_pages = None
            if total_items is not None and per_page:
                total_pages = (total_items + per_page - 1) // per_page

            if not items:
                break

            if total_pages is not None and page >= total_pages:
                break

            page += 1

        return collections

    def _authenticate(self):
        payload = {"identity": self.email, "password": self.password}
        response = self._request("POST", self.AUTH_ENDPOINT, json=payload, authenticate=False)
        token = response.get("token")
        if not token:
            raise RuntimeError("PocketBase did not return an authentication token.")

        self.token = token

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json: Optional[Dict] = None,
        data: Optional[Dict] = None,
        authenticate: bool = True,
    ) -> Dict:
        url = f"{self.base_url}{endpoint}"
        headers = {}
        if authenticate:
            if not self.token:
                self._authenticate()
            headers["Authorization"] = f"Bearer {self.token}"

        response = self.session.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json,
            data=data,
            headers=headers or None,
            timeout=30,
        )

        if not response.ok:
            raise RuntimeError(
                f"PocketBase request failed ({response.status_code}): {response.text.strip()}"
            )

        if response.status_code == 204 or not response.content:
            return {}

        return response.json()
