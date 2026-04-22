from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import requests
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.meta_ad_library_handler.tables import AdsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MetaAdLibraryHandler(APIHandler):
    """Read-only handler for Meta's public Ad Library archive."""

    name = "meta_ad_library"

    DEFAULT_API_VERSION = "v24.0"
    DEFAULT_AD_REACHED_COUNTRIES = ["ALL"]
    DEFAULT_AD_TYPE = "ALL"
    DEFAULT_AD_ACTIVE_STATUS = "ALL"
    DEFAULT_SEARCH_TYPE = "KEYWORD_UNORDERED"
    DEFAULT_PAGE_SIZE = 100
    DEFAULT_LOCAL_FILTER_SCAN_PAGES = 5
    PUBLIC_AD_LIBRARY_URL = "https://www.facebook.com/ads/library/"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})
        self.session: requests.Session | None = None

        self.access_token = self.connection_data.get("access_token")
        self.api_version = self.connection_data.get("api_version", self.DEFAULT_API_VERSION)
        self.base_url = f"https://graph.facebook.com/{self.api_version}/ads_archive"

        self._register_table("ads", AdsTable(self))

    def connect(self) -> None:
        if not self.access_token:
            raise ValueError("access_token is required")
        if self.session is None:
            self.session = requests.Session()
        self.is_connected = True

    def disconnect(self) -> None:
        if self.session is not None:
            self.session.close()
            self.session = None
        super().disconnect()

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(success=False)
        try:
            self.connect()
            self.fetch_ads(limit=1)
            response.success = True
        except Exception as exc:  # noqa: BLE001
            logger.error("Error connecting to Meta Ad Library: %s", exc)
            response.error_message = str(exc)
            self.disconnect()
        return response

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query)
        return self.query(ast)

    def fetch_ads(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        self.connect()

        conditions = conditions or []
        params = self._build_request_params(conditions=conditions, limit=limit)
        has_local_filters = self._has_unapplied_conditions(conditions)
        if has_local_filters and limit is not None:
            params["limit"] = self.DEFAULT_PAGE_SIZE

        response_rows: list[dict[str, Any]] = []
        next_cursor: str | None = None
        pages_scanned = 0

        while True:
            request_params = dict(params)
            if next_cursor is not None:
                request_params["after"] = next_cursor

            payload = self._request(request_params)
            pages_scanned += 1
            rows = payload.get("data", [])
            response_rows.extend(self._normalize_row(row) for row in rows)

            if not has_local_filters and limit is not None and len(response_rows) >= limit:
                return response_rows[:limit]

            next_cursor = payload.get("paging", {}).get("cursors", {}).get("after")
            if not next_cursor or not rows:
                return response_rows
            if has_local_filters and limit is not None and pages_scanned >= self.DEFAULT_LOCAL_FILTER_SCAN_PAGES:
                return response_rows

    def _build_request_params(
        self,
        conditions: list[FilterCondition] | None,
        limit: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "fields": ",".join(AdsTable.COLUMNS),
            "access_token": self.access_token,
            "ad_reached_countries": json.dumps(
                self._coerce_to_list(
                    self.connection_data.get("ad_reached_countries"),
                    fallback=self.DEFAULT_AD_REACHED_COUNTRIES,
                )
            ),
            "ad_type": self.connection_data.get("ad_type", self.DEFAULT_AD_TYPE),
            "ad_active_status": self.connection_data.get(
                "ad_active_status",
                self.DEFAULT_AD_ACTIVE_STATUS,
            ),
            "limit": min(limit or self.DEFAULT_PAGE_SIZE, self.DEFAULT_PAGE_SIZE),
            "unmask_removed_content": str(
                bool(self.connection_data.get("unmask_removed_content", False))
            ).lower(),
        }

        search_page_ids = self._coerce_to_list(self.connection_data.get("search_page_ids"))
        if search_page_ids:
            params["search_page_ids"] = json.dumps(search_page_ids)

        search_terms = self.connection_data.get("search_terms")
        if search_terms:
            params["search_terms"] = search_terms
            params["search_type"] = self.connection_data.get(
                "search_type",
                self.DEFAULT_SEARCH_TYPE,
            )

        languages = self._coerce_to_list(self.connection_data.get("languages"))
        if languages:
            params["languages"] = json.dumps(languages)

        publisher_platforms = self._coerce_to_list(self.connection_data.get("publisher_platforms"))
        if publisher_platforms:
            params["publisher_platforms"] = json.dumps(publisher_platforms)

        date_min, date_max = self._extract_delivery_date_bounds(conditions or [])
        if date_min is not None:
            params["ad_delivery_date_min"] = date_min.isoformat()
        if date_max is not None:
            params["ad_delivery_date_max"] = date_max.isoformat()

        return params

    @staticmethod
    def _has_unapplied_conditions(conditions: list[FilterCondition]) -> bool:
        return any(not getattr(condition, "applied", False) for condition in conditions)

    def _request(self, params: dict[str, Any]) -> dict[str, Any]:
        if self.session is None:
            raise RuntimeError("Handler is not connected. Call connect() first.")
        response = self.session.get(self.base_url, params=params, timeout=30)
        if response.ok:
            return response.json()

        message = response.text
        try:
            payload = response.json()
            error = payload.get("error", {})
            if error:
                message = error.get("message", response.text)
        except ValueError:
            pass

        raise RuntimeError(f"Meta Ad Library request failed ({response.status_code}): {message}")

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for column in AdsTable.COLUMNS:
            value = row.get(column)
            if column == "ad_snapshot_url":
                value = self._sanitize_ad_snapshot_url(value)
            normalized[column] = value
        return normalized

    @classmethod
    def _sanitize_ad_snapshot_url(cls, snapshot_url: Any) -> Any:
        if not isinstance(snapshot_url, str):
            return snapshot_url

        parsed = urlparse(snapshot_url)
        query = parse_qs(parsed.query)
        ad_ids = query.get("id")

        host = parsed.netloc.split(":", 1)[0].lower()
        if (
            ad_ids
            and ad_ids[0]
            and host.endswith("facebook.com")
            and parsed.path.rstrip("/") == "/ads/archive/render_ad"
        ):
            return f"{cls.PUBLIC_AD_LIBRARY_URL}?{urlencode({'id': ad_ids[0]})}"

        return cls._remove_access_token_from_url(snapshot_url)

    @staticmethod
    def _remove_access_token_from_url(url: str) -> str:
        parsed = urlparse(url)
        query_items = parse_qsl(parsed.query, keep_blank_values=True)
        if not any(key.lower() == "access_token" for key, _ in query_items):
            return url

        filtered_items = [
            (key, value)
            for key, value in query_items
            if key.lower() != "access_token"
        ]
        return urlunparse(parsed._replace(query=urlencode(filtered_items)))

    def _extract_delivery_date_bounds(
        self,
        conditions: list[FilterCondition],
    ) -> tuple[date | None, date | None]:
        date_min: date | None = None
        date_max: date | None = None

        for condition in conditions:
            if condition.column not in {"ad_creation_time", "ad_delivery_start_time", "ad_delivery_stop_time"}:
                continue

            values: list[date]
            if condition.op == FilterOperator.BETWEEN:
                if not isinstance(condition.value, (list, tuple)) or len(condition.value) != 2:
                    continue
                values = [self._coerce_date(condition.value[0]), self._coerce_date(condition.value[1])]
            else:
                values = [self._coerce_date(condition.value)]

            if any(value is None for value in values):
                continue

            if condition.op in {FilterOperator.GREATER_THAN, FilterOperator.GREATER_THAN_OR_EQUAL}:
                date_min = self._max_date(date_min, values[0])
                condition.applied = True
            elif condition.op in {FilterOperator.LESS_THAN, FilterOperator.LESS_THAN_OR_EQUAL}:
                date_max = self._min_date(date_max, values[0])
                condition.applied = True
            elif condition.op == FilterOperator.BETWEEN:
                date_min = self._max_date(date_min, values[0])
                date_max = self._min_date(date_max, values[1])
                condition.applied = True

        return date_min, date_max

    @staticmethod
    def _coerce_to_list(value: Any, fallback: list[str] | None = None) -> list[str]:
        if value is None:
            return list(fallback or [])
        if isinstance(value, (list, tuple)):
            return [str(item) for item in value]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return list(fallback or [])
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    return [stripped]
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            return [stripped]
        return [str(value)]

    @staticmethod
    def _coerce_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return None
            candidate = candidate.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(candidate).astimezone(timezone.utc).date()
            except ValueError:
                try:
                    return date.fromisoformat(candidate[:10])
                except ValueError:
                    return None
        return None

    @staticmethod
    def _max_date(current: date | None, candidate: date | None) -> date | None:
        if candidate is None:
            return current
        if current is None:
            return candidate
        return max(current, candidate)

    @staticmethod
    def _min_date(current: date | None, candidate: date | None) -> date | None:
        if candidate is None:
            return current
        if current is None:
            return candidate
        return min(current, candidate)
