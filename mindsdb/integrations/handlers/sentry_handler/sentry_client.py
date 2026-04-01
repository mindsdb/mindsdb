from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import unquote

import requests


RETRYABLE_STATUS_CODES = {429, 502, 503, 504}
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PAGE_SIZE = 100
DEFAULT_ISSUES_LIMIT = 100
MAX_ISSUES_LIMIT = 1000


class SentryRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        operation: str,
        status_code: int | None = None,
        response_text: str | None = None,
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.status_code = status_code
        self.response_text = response_text


class SentryClient:
    def __init__(
        self,
        *,
        auth_token: str,
        organization_slug: str,
        project_slug: str,
        environment: str,
        base_url: str = "https://sentry.io",
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = 3,
        session: requests.Session | None = None,
        sleep=time.sleep,
    ) -> None:
        self.organization_slug = organization_slug
        self.project_slug = project_slug
        self.environment = environment
        self.base_url = base_url.rstrip("/")
        self.api_base = f"{self.base_url}/api/0"
        self.timeout = timeout
        self.max_retries = max_retries
        self._sleep = sleep
        self._resolved_project: dict[str, Any] | None = None

        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {auth_token}",
                "Accept": "application/json",
            }
        )

    def list_projects(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        return self._paginate(
            f"/organizations/{self.organization_slug}/projects/",
            limit=limit,
            operation="projects",
        )

    def resolve_project(self) -> dict[str, Any]:
        if self._resolved_project is not None:
            return self._resolved_project

        for project in self.list_projects():
            if project.get("slug") == self.project_slug:
                self._resolved_project = project
                return project

        raise ValueError(
            f"Failed to resolve Sentry project slug '{self.project_slug}' "
            f"in organization '{self.organization_slug}'"
        )

    def list_issues(
        self,
        *,
        project_id: int | str,
        query: str = "",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        total_limit = DEFAULT_ISSUES_LIMIT if limit is None else min(limit, MAX_ISSUES_LIMIT)
        return self._paginate(
            f"/organizations/{self.organization_slug}/issues/",
            params={"project": project_id, "query": self._apply_environment_query(query)},
            limit=total_limit,
            operation="issues",
        )

    def validate_connection(self) -> dict[str, Any]:
        project = self.resolve_project()
        self.list_issues(project_id=project.get("id"), query="", limit=1)
        return project

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        operation: str,
    ) -> tuple[Any, requests.Response]:
        return self._request(method, path, params=params, operation=operation)

    def _paginate(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        limit: int | None = None,
        operation: str,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        cursor: str | None = None
        remaining = limit

        while True:
            request_params = dict(params or {})
            page_limit = DEFAULT_PAGE_SIZE if remaining is None else min(DEFAULT_PAGE_SIZE, remaining)
            request_params["limit"] = page_limit
            if cursor:
                request_params["cursor"] = cursor

            payload, response = self._request("GET", path, params=request_params, operation=operation)
            if not isinstance(payload, list):
                raise RuntimeError(f"Sentry {operation} request returned malformed payload")

            results.extend(payload)
            if remaining is not None:
                remaining -= len(payload)
                if remaining <= 0:
                    break

            if len(payload) == 0:
                break

            cursor = self._extract_next_cursor(response.headers.get("Link"))
            if not cursor:
                break

        return results if limit is None else results[:limit]

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        operation: str,
    ) -> tuple[Any, requests.Response]:
        url = f"{self.api_base}{path}"

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(method, url, params=params, timeout=self.timeout)
            except requests.Timeout as exc:
                if attempt >= self.max_retries:
                    raise SentryRequestError(
                        f"Sentry {operation} request timed out",
                        operation=operation,
                    ) from exc
                self._sleep(2**attempt)
                continue
            except requests.RequestException as exc:
                raise SentryRequestError(
                    f"Sentry {operation} request failed: {exc}",
                    operation=operation,
                ) from exc

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                self._sleep(2**attempt)
                continue

            if response.status_code >= 400:
                details = response.text.strip() if getattr(response, "text", None) else None
                message = f"Sentry {operation} request failed with status {response.status_code}"
                if details:
                    message = f"{message}: {details[:500]}"
                raise SentryRequestError(
                    message,
                    operation=operation,
                    status_code=response.status_code,
                    response_text=details,
                )

            try:
                return response.json(), response
            except ValueError as exc:
                raise SentryRequestError(
                    f"Sentry {operation} request returned malformed JSON",
                    operation=operation,
                    status_code=response.status_code,
                ) from exc

        raise SentryRequestError(
            f"Sentry {operation} request failed after retries",
            operation=operation,
        )

    def _apply_environment_query(self, query: str) -> str:
        environment_fragment = f"environment:{self._quote_query_value(self.environment)}"
        stripped_query = query.strip()
        if not stripped_query:
            return environment_fragment
        return f"{environment_fragment} {stripped_query}"

    @staticmethod
    def _quote_query_value(value: str) -> str:
        escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped_value}"'

    @staticmethod
    def _extract_next_cursor(link_header: str | None) -> str | None:
        if not link_header:
            return None

        for part in link_header.split(","):
            if 'rel="next"' not in part or 'results="true"' not in part:
                continue

            cursor_match = re.search(r'cursor="([^"]+)"', part)
            if cursor_match:
                return cursor_match.group(1)

            url_match = re.search(r"[?&]cursor=([^&>]+)", part)
            if url_match:
                return unquote(url_match.group(1))

        return None
