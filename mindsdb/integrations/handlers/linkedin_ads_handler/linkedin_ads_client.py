"""LinkedIn Ads API client for making authenticated requests.

Handles HTTP communication, pagination, and collection fetching.
"""

from __future__ import annotations

from typing import Any

import requests
from mindsdb.integrations.handlers.linkedin_ads_handler.linkedin_ads_auth import LinkedInAdsAuthManager
from mindsdb.integrations.utilities.sql_utils import SortColumn
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class LinkedInAdsClient:
    """HTTP client for LinkedIn Ads API.

    Responsibilities:
    - Authenticated HTTP requests with automatic token refresh on 401
    - Paginated collection fetching with configurable limits
    - Request header management
    - Error handling for API responses
    """

    RESTLI_PROTOCOL_VERSION = "2.0.0"
    DEFAULT_PAGE_SIZE = 100
    MAX_PAGE_SIZE = 1000

    def __init__(
        self,
        auth_manager: LinkedInAdsAuthManager,
        api_version: str = "202602",
    ):
        """Initialize client.

        Args:
            auth_manager: Auth manager for obtaining valid access tokens
            api_version: LinkedIn Marketing API version (e.g., "202602")
        """
        self.auth_manager = auth_manager
        self.api_version = api_version
        self._session: requests.Session | None = None

    def connect(self) -> requests.Session:
        """Create or return existing session with auth headers.

        Returns:
            Configured requests session
        """
        if self._session is not None:
            return self._session

        access_token = self.auth_manager.get_valid_access_token()
        session = requests.Session()
        session.headers.update(self._build_headers(access_token))
        self._session = session
        return session

    def disconnect(self) -> None:
        """Close the session and cleanup."""
        if self._session is not None:
            self._session.close()
            self._session = None

    def request_json(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make an authenticated JSON request to LinkedIn Ads API.

        Automatically retries once with refreshed token on 401 response.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            params: Query parameters
            extra_headers: Additional headers to include

        Returns:
            Parsed JSON response as dictionary

        Raises:
            RuntimeError: If API returns an error status
            ValueError: If response is not valid JSON dict
        """
        session = self.connect()
        request_headers = dict(extra_headers or {})
        response = session.request(
            method=method,
            url=url,
            params=params,
            headers=request_headers or None,
            timeout=30,
        )

        # Retry once with refreshed token on 401
        if response.status_code == 401:
            logger.info("Refreshing LinkedIn Ads access token after 401 response")
            try:
                new_token_data = self.auth_manager.refresh_if_needed()
                access_token = new_token_data.get("access_token")
                if access_token:
                    self.disconnect()
                    session = self.connect()
                    response = session.request(
                        method=method,
                        url=url,
                        params=params,
                        headers=request_headers or None,
                        timeout=30,
                    )
            except Exception as exc:
                logger.error("Failed to refresh token after 401: %s", exc)
                raise

        if not response.ok:
            raise RuntimeError(f"LinkedIn Ads API error {response.status_code}: {response.text}")

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected LinkedIn Ads API response format")
        return payload

    def fetch_collection(
        self,
        endpoint: str,
        search_filters: dict[str, list[str]] | None = None,
        params: dict[str, Any] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a paginated collection from LinkedIn Ads API.

        Supports three query modes:
        1. Custom params (when params is provided)
        2. Search filters (when search_filters is provided)
        3. Default search (when neither is provided)

        Args:
            endpoint: API endpoint URL
            search_filters: Search filters as dict of field -> values
            params: Custom query parameters (overrides search_filters)
            limit: Maximum number of results to fetch
            sort: Sort columns (first column determines sort order)
            extra_headers: Additional headers for the request

        Returns:
            List of result items (elements from API response)
        """
        from mindsdb.integrations.handlers.linkedin_ads_handler.linkedin_ads_utils import (
            build_search_expression,
            build_search_request_url,
            build_sort_order,
        )

        rows: list[dict[str, Any]] = []
        desired = limit or self.DEFAULT_PAGE_SIZE
        if desired <= 0:
            return rows

        next_page_token: str | None = None
        while len(rows) < desired:
            page_size = min(self.MAX_PAGE_SIZE, desired - len(rows))
            sort_order = build_sort_order(sort)
            request_url = endpoint
            request_params: dict[str, Any] | None

            # Mode 1: Custom params provided
            if params is not None:
                request_params = dict(params)
                request_params["pageSize"] = page_size
                request_params.setdefault("sortOrder", sort_order)
                if next_page_token:
                    request_params["pageToken"] = next_page_token

            # Mode 2: Search filters provided
            elif search_filters:
                request_params = None
                search_expression = build_search_expression(search_filters)
                request_url = build_search_request_url(
                    endpoint=endpoint,
                    search_expression=search_expression,
                    page_size=page_size,
                    sort_order=sort_order,
                    page_token=next_page_token,
                )

            # Mode 3: Default search
            else:
                request_params = {"q": "search", "pageSize": page_size, "sortOrder": sort_order}
                if next_page_token:
                    request_params["pageToken"] = next_page_token

            payload = self.request_json(
                "GET",
                request_url,
                params=request_params,
                extra_headers=extra_headers,
            )

            # Extract elements from response
            elements = payload.get("elements") or payload.get("results") or []
            if isinstance(elements, dict):
                elements = list(elements.values())
            if not isinstance(elements, list):
                raise ValueError("LinkedIn Ads API returned an unexpected collection payload")
            rows.extend(elements)

            # Check for next page
            paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
            next_page_token = paging.get("nextPageToken")
            if not next_page_token or not elements:
                break

        return rows[:desired]

    def fetch_by_ids(self, endpoint: str, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch multiple items by ID using batch endpoint.

        Args:
            endpoint: API endpoint URL
            ids: List of IDs to fetch

        Returns:
            List of result items

        Raises:
            ValueError: If API response format is unexpected
        """
        if not ids:
            return []

        ids_list = ",".join(str(id_val) for id_val in ids)
        request_url = f"{endpoint}?ids=List({ids_list})"
        payload = self.request_json("GET", request_url)
        results = payload.get("results", {})
        if not isinstance(results, dict):
            raise ValueError("LinkedIn Ads batch response results is not a dict")
        return [value for value in results.values() if isinstance(value, dict)]

    def _build_headers(self, access_token: str) -> dict[str, str]:
        """Build request headers with authentication.

        Args:
            access_token: Valid access token

        Returns:
            Dictionary of HTTP headers
        """
        return {
            "Authorization": f"Bearer {access_token}",
            "LinkedIn-Version": str(self.api_version),
            "X-Restli-Protocol-Version": self.RESTLI_PROTOCOL_VERSION,
            "Accept": "application/json",
        }
