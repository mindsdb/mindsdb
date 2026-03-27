from __future__ import annotations

import threading
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote
from typing import Any

import requests
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.linkedin_ads_handler.tables import (
    CampaignAnalyticsTable,
    CampaignGroupsTable,
    CampaignsTable,
    CreativesTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class LinkedInAdsHandler(APIHandler):
    """Handler for LinkedIn Ads account-scoped discovery and metadata tables."""

    name = "linkedin_ads"

    TOKEN_ENDPOINT = "https://www.linkedin.com/oauth/v2/accessToken"
    AD_ACCOUNTS_ENDPOINT = "https://api.linkedin.com/rest/adAccounts"
    CAMPAIGNS_ENDPOINT = "https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns"
    CAMPAIGN_GROUPS_ENDPOINT = "https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaignGroups"
    CREATIVES_ENDPOINT = "https://api.linkedin.com/rest/adAccounts/{account_id}/creatives"
    AD_ANALYTICS_ENDPOINT = "https://api.linkedin.com/rest/adAnalytics"

    RESTLI_PROTOCOL_VERSION = "2.0.0"
    DEFAULT_API_VERSION = "202602"
    DEFAULT_PAGE_SIZE = 100
    MAX_PAGE_SIZE = 1000
    TOKEN_EXPIRY_SKEW_SECONDS = 300
    TOKEN_STORAGE_KEY = "linkedin_ads_tokens"
    DEFAULT_ANALYTICS_LOOKBACK_DAYS = 30
    DEFAULT_ANALYTICS_TIME_GRANULARITY = "DAILY"
    SUPPORTED_TIME_GRANULARITIES = {"ALL", "DAILY", "MONTHLY"}
    CAMPAIGN_ANALYTICS_FIELDS = (
        "dateRange",
        "pivotValues",
        "impressions",
        "clicks",
        "landingPageClicks",
        "likes",
        "shares",
        "costInLocalCurrency",
        "externalWebsiteConversions",
    )
    ALL_STATUSES = (
        "ACTIVE",
        "ARCHIVED",
        "CANCELED",
        "DRAFT",
        "PAUSED",
        "PENDING_DELETION",
        "REMOVED",
    )

    CAMPAIGN_SEARCH_FILTERS = {
        "id": "id",
        "status": "status",
        "name": "name",
        "test": "test",
    }
    CAMPAIGN_GROUP_SEARCH_FILTERS = {
        "id": "id",
        "status": "status",
        "name": "name",
        "test": "test",
    }
    CREATIVE_CRITERIA_FILTERS = {
        "id": "creatives",
        "creative_id": "creatives",
        "campaign_urn": "campaigns",
        "campaign_id": "campaigns",
        "content_reference": "contentReferences",
        "content_reference_id": "contentReferences",
        "intended_status": "intendedStatuses",
        "is_test": "isTestAccount",
    }

    _refresh_lock = threading.Lock()

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})
        self.handler_storage = kwargs.get("handler_storage")

        self.account_id = self.connection_data.get("account_id")
        self.access_token = self.connection_data.get("access_token")
        self.refresh_token = self.connection_data.get("refresh_token")
        self.client_id = self.connection_data.get("client_id")
        self.client_secret = self.connection_data.get("client_secret")
        self.api_version = self.connection_data.get("api_version", self.DEFAULT_API_VERSION)

        self.connection: requests.Session | None = None
        self._token_data: dict[str, Any] | None = None

        self._register_table("campaigns", CampaignsTable(self))
        self._register_table("campaign_groups", CampaignGroupsTable(self))
        self._register_table("creatives", CreativesTable(self))
        self._register_table("campaign_analytics", CampaignAnalyticsTable(self))

    def connect(self) -> requests.Session:
        if self.is_connected and self.connection is not None:
            return self.connection

        if not self.account_id:
            raise ValueError("account_id is required")

        token_data = self._get_valid_token()
        access_token = token_data.get("access_token")
        if not access_token:
            raise ValueError("A valid access_token could not be obtained")

        session = requests.Session()
        session.headers.update(self._build_headers(access_token))
        self.connection = session
        self._token_data = token_data
        self.is_connected = True
        return session

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        super().disconnect()

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(success=False)
        try:
            self.connect()
            self._get_account_detail()
            response.success = True
        except Exception as exc:  # noqa: BLE001
            logger.error("Error connecting to LinkedIn Ads: %s", exc)
            response.success = False
            response.error_message = str(exc)
            self.disconnect()
        return response

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query)
        return self.query(ast)

    def fetch_campaigns(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        endpoint = self.CAMPAIGNS_ENDPOINT.format(account_id=self.account_id)
        search_filters = self._extract_search_filters(conditions, self.CAMPAIGN_SEARCH_FILTERS)
        search_filters = self._normalize_search_id_filters(search_filters, "urn:li:sponsoredCampaign:")
        elements = self._fetch_collection(endpoint, search_filters=search_filters, limit=limit, sort=sort)
        return [self._normalize_campaign(item) for item in elements]

    def fetch_campaign_groups(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        endpoint = self.CAMPAIGN_GROUPS_ENDPOINT.format(account_id=self.account_id)
        search_filters = self._extract_search_filters(conditions, self.CAMPAIGN_GROUP_SEARCH_FILTERS)
        id_values = self._extract_search_id_values(search_filters)

        if id_values:
            elements = self._fetch_campaign_groups_by_ids(endpoint, id_values)
            rows = [self._normalize_campaign_group(item) for item in elements]
            rows = self._filter_campaign_groups_locally(rows, search_filters)
            rows = self._sort_rows_locally(rows, sort)
            return rows[:limit] if limit is not None else rows

        elements = self._fetch_collection(endpoint, search_filters=search_filters, limit=limit, sort=sort)
        return [self._normalize_campaign_group(item) for item in elements]

    def fetch_creatives(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        endpoint = self.CREATIVES_ENDPOINT.format(account_id=self.account_id)
        params = self._build_creative_params(conditions, sort=sort)
        elements = self._fetch_collection(
            endpoint,
            params=params,
            limit=limit,
            sort=sort,
            extra_headers={"X-RestLi-Method": "FINDER"},
        )
        return [self._normalize_creative(item) for item in elements]

    def fetch_campaign_analytics(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        params = self._build_campaign_analytics_params(conditions)
        request_url = self._build_raw_request_url(self.AD_ANALYTICS_ENDPOINT, params)
        payload = self._request_json("GET", request_url)
        elements = payload.get("elements", [])
        if not isinstance(elements, list):
            raise ValueError("LinkedIn Ads analytics response elements is not a list")

        rows = [self._normalize_campaign_analytics(item, params["timeGranularity"]) for item in elements]
        if sort:
            for sort_column in sort:
                if sort_column.column in CampaignAnalyticsTable.COLUMNS:
                    sort_column.applied = False
        return rows[:limit] if limit is not None else rows

    def _fetch_collection(
        self,
        endpoint: str,
        search_filters: dict[str, list[str]] | None = None,
        params: dict[str, Any] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        desired = limit or self.DEFAULT_PAGE_SIZE
        if desired <= 0:
            return rows

        next_page_token: str | None = None
        while len(rows) < desired:
            page_size = min(self.MAX_PAGE_SIZE, desired - len(rows))
            sort_order = self._build_sort_order(sort)
            request_url = endpoint
            request_params: dict[str, Any] | None
            if params is not None:
                request_params = dict(params)
                request_params["pageSize"] = page_size
                request_params.setdefault("sortOrder", sort_order)
                if next_page_token:
                    request_params["pageToken"] = next_page_token
            elif search_filters:
                request_params = None
                request_url = self._build_search_request_url(
                    endpoint=endpoint,
                    search_expression=self._build_search_expression(search_filters),
                    page_size=page_size,
                    sort_order=sort_order,
                    page_token=next_page_token,
                )
            else:
                request_params = {"q": "search", "pageSize": page_size, "sortOrder": sort_order}
                if next_page_token:
                    request_params["pageToken"] = next_page_token

            payload = self._request_json(
                "GET",
                request_url,
                params=request_params,
                extra_headers=extra_headers,
            )
            elements = payload.get("elements") or payload.get("results") or []
            if isinstance(elements, dict):
                elements = list(elements.values())
            if not isinstance(elements, list):
                raise ValueError("LinkedIn Ads API returned an unexpected collection payload")
            rows.extend(elements)

            paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
            next_page_token = paging.get("nextPageToken")
            if not next_page_token or not elements:
                break

        return rows[:desired]

    def _fetch_campaign_groups_by_ids(self, endpoint: str, ids: list[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        ids_list = ",".join(str(self._extract_urn_id(value)) for value in ids)
        request_url = f"{endpoint}?ids=List({ids_list})"
        payload = self._request_json("GET", request_url)
        results = payload.get("results", {})
        if not isinstance(results, dict):
            raise ValueError("LinkedIn Ads campaign group batch response results is not a dict")
        return [value for value in results.values() if isinstance(value, dict)]

    def _request_json(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        session = self.connect()
        request_headers = dict(extra_headers or {})
        response = session.request(
            method=method,
            url=url,
            params=params,
            headers=request_headers or None,
            timeout=30,
        )

        if response.status_code == 401 and self.refresh_token and self.client_id and self.client_secret:
            logger.info("Refreshing LinkedIn Ads access token after 401 response")
            self._refresh_connection_tokens()
            session = self.connect()
            response = session.request(
                method=method,
                url=url,
                params=params,
                headers=request_headers or None,
                timeout=30,
            )

        if not response.ok:
            raise RuntimeError(f"LinkedIn Ads API error {response.status_code}: {response.text}")

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected LinkedIn Ads API response format")
        return payload

    def _get_valid_token(self) -> dict[str, Any]:
        stored_token_data = self._load_stored_tokens()
        if stored_token_data:
            token_data = stored_token_data
        else:
            if not self.access_token and not self.refresh_token:
                raise ValueError("At least access_token or refresh_token must be provided for authentication")
            token_data = {
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "expires_at": None,
                "refresh_token_expires_at": None,
            }
            self._store_tokens(token_data)

        if self._token_is_expired(token_data) and token_data.get("refresh_token"):
            if not self.client_id or not self.client_secret:
                raise ValueError("client_id and client_secret are required to refresh an expired LinkedIn token")
            with self._refresh_lock:
                latest_tokens = self._load_stored_tokens() or token_data
                if self._token_is_expired(latest_tokens):
                    token_data = self._refresh_tokens(latest_tokens["refresh_token"])
                    self._store_tokens(token_data)
                else:
                    token_data = latest_tokens
        elif not token_data.get("access_token") and token_data.get("refresh_token"):
            if not self.client_id or not self.client_secret:
                raise ValueError("client_id and client_secret are required to exchange a LinkedIn refresh token")
            with self._refresh_lock:
                token_data = self._refresh_tokens(token_data["refresh_token"])
                self._store_tokens(token_data)

        self.access_token = token_data.get("access_token")
        self.refresh_token = token_data.get("refresh_token")
        return token_data

    def _refresh_connection_tokens(self) -> None:
        if not self.refresh_token:
            raise ValueError("No refresh_token available for LinkedIn Ads token refresh")
        if not self.client_id or not self.client_secret:
            raise ValueError("client_id and client_secret are required for LinkedIn Ads token refresh")

        with self._refresh_lock:
            token_data = self._refresh_tokens(self.refresh_token)
            self._store_tokens(token_data)
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token")
            self.disconnect()

    def _refresh_tokens(self, refresh_token: str) -> dict[str, Any]:
        response = requests.post(
            self.TOKEN_ENDPOINT,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"LinkedIn Ads API error {response.status_code}: {response.text}")

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected LinkedIn token refresh response format")

        new_refresh_token = payload.get("refresh_token") or refresh_token
        return {
            "access_token": payload.get("access_token"),
            "refresh_token": new_refresh_token,
            "expires_at": self._build_expiry(payload.get("expires_in")),
            "refresh_token_expires_at": self._build_expiry(payload.get("refresh_token_expires_in")),
        }

    def _load_stored_tokens(self) -> dict[str, Any] | None:
        if self.handler_storage is None:
            return None
        try:
            payload = self.handler_storage.encrypted_json_get(self.TOKEN_STORAGE_KEY)
        except Exception as exc:  # noqa: BLE001
            logger.debug("No stored LinkedIn Ads tokens found: %s", exc)
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _store_tokens(self, token_data: dict[str, Any]) -> None:
        if self.handler_storage is None:
            return
        payload = dict(token_data)
        for key in ("expires_at", "refresh_token_expires_at"):
            value = payload.get(key)
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
        self.handler_storage.encrypted_json_set(self.TOKEN_STORAGE_KEY, payload)

    @staticmethod
    def _build_expiry(seconds: Any) -> datetime | None:
        if not seconds:
            return None
        try:
            return datetime.now(timezone.utc) + timedelta(seconds=int(seconds))
        except (TypeError, ValueError):
            return None

    def _token_is_expired(self, token_data: dict[str, Any]) -> bool:
        expires_at = token_data.get("expires_at")
        if not expires_at:
            return False
        expiry = expires_at
        if isinstance(expires_at, str):
            expiry = datetime.fromisoformat(expires_at)
        if isinstance(expiry, datetime):
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc) + timedelta(seconds=self.TOKEN_EXPIRY_SKEW_SECONDS)
            return expiry <= now
        return False

    def _build_headers(self, access_token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {access_token}",
            "LinkedIn-Version": str(self.api_version),
            "X-Restli-Protocol-Version": self.RESTLI_PROTOCOL_VERSION,
            "Accept": "application/json",
        }

    def _get_account_detail(self) -> dict[str, Any]:
        payload = self._request_json(
            "GET",
            f"{self.AD_ACCOUNTS_ENDPOINT}/{self.account_id}",
        )
        return payload

    @staticmethod
    def _build_sort_order(sort: list[SortColumn] | None) -> str:
        if not sort:
            return "ASCENDING"
        direction = getattr(sort[0], "direction", None)
        return "DESCENDING" if str(direction).lower() == "desc" else "ASCENDING"

    @classmethod
    def _extract_search_filters(
        cls,
        conditions: list[FilterCondition] | None,
        allowed_filters: dict[str, str],
    ) -> dict[str, list[str]] | None:
        if not conditions:
            return None

        filters: dict[str, list[str]] = {}
        for condition in conditions:
            field = allowed_filters.get(condition.column)
            if field is None:
                continue

            values: list[str] | None = None
            if condition.op == FilterOperator.EQUAL:
                values = [cls._format_search_value(condition.value)]
            elif condition.op == FilterOperator.IN and isinstance(condition.value, list):
                values = [cls._format_search_value(value) for value in condition.value]

            if not values:
                continue

            filters.setdefault(field, []).extend(values)
            condition.applied = True

        return filters or None

    @staticmethod
    def _extract_search_id_values(search_filters: dict[str, list[str]] | None) -> list[str] | None:
        if not search_filters:
            return None
        values = search_filters.get("id")
        if not values:
            return None
        return list(values)

    @classmethod
    def _normalize_search_id_filters(
        cls,
        search_filters: dict[str, list[str]] | None,
        prefix: str,
    ) -> dict[str, list[str]] | None:
        if not search_filters or "id" not in search_filters:
            return search_filters

        normalized = dict(search_filters)
        normalized["id"] = [
            value if str(value).startswith(prefix) else f"{prefix}{value}"
            for value in search_filters["id"]
        ]
        return normalized

    @classmethod
    def _build_search_expression(cls, search_filters: dict[str, list[str]]) -> str:
        parts = [
            f"{field}:(values:{cls._restli_list(values)})"
            for field, values in search_filters.items()
            if values
        ]
        return f"({','.join(parts)})" if parts else ""

    @classmethod
    def _build_search_request_url(
        cls,
        endpoint: str,
        search_expression: str,
        page_size: int,
        sort_order: str,
        page_token: str | None = None,
    ) -> str:
        url = (
            f"{endpoint}?q=search&search={search_expression}"
            f"&pageSize={page_size}&sortOrder={sort_order}"
        )
        if page_token:
            url += f"&pageToken={page_token}"
        return url

    @staticmethod
    def _build_raw_request_url(endpoint: str, params: dict[str, Any]) -> str:
        query_parts = [f"{key}={value}" for key, value in params.items()]
        return f"{endpoint}?{'&'.join(query_parts)}"

    @staticmethod
    def _format_search_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    @classmethod
    def _filter_campaign_groups_locally(
        cls,
        rows: list[dict[str, Any]],
        search_filters: dict[str, list[str]] | None,
    ) -> list[dict[str, Any]]:
        if not search_filters:
            return rows

        filtered = rows
        for field, values in search_filters.items():
            if not values or field == "id":
                continue

            if field == "status":
                allowed = {str(value).upper() for value in values}
                filtered = [row for row in filtered if str(row.get("status", "")).upper() in allowed]
                continue

            if field == "name":
                allowed = {str(value) for value in values}
                filtered = [row for row in filtered if str(row.get("name")) in allowed]
                continue

            if field == "test":
                allowed = {str(value).lower() == "true" for value in values}
                filtered = [row for row in filtered if bool(row.get("test")) in allowed]

        return filtered

    @staticmethod
    def _sort_rows_locally(
        rows: list[dict[str, Any]],
        sort: list[SortColumn] | None,
    ) -> list[dict[str, Any]]:
        if not sort:
            return rows

        sorted_rows = list(rows)
        for sort_column in reversed(sort):
            column = getattr(sort_column, "column", None)
            if not column:
                continue
            direction = getattr(sort_column, "direction", None)
            reverse = str(direction).lower() == "desc"
            sorted_rows.sort(key=lambda row: row.get(column), reverse=reverse)
            sort_column.applied = True
        return sorted_rows

    def _build_creative_params(
        self,
        conditions: list[FilterCondition] | None,
        sort: list[SortColumn] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"q": "criteria"}
        if sort:
            sort_order = self._build_sort_order(sort)
            params["sortOrder"] = sort_order
            for sort_column in sort:
                if sort_column.column in CreativesTable.COLUMNS:
                    sort_column.applied = True

        for condition in conditions or []:
            field = self.CREATIVE_CRITERIA_FILTERS.get(condition.column)
            if field is None:
                continue

            values: list[str] | None = None
            if condition.op == FilterOperator.EQUAL:
                values = [self._format_creative_filter_value(condition.column, condition.value)]
            elif condition.op == FilterOperator.IN and isinstance(condition.value, list):
                values = [self._format_creative_filter_value(condition.column, value) for value in condition.value]

            if not values:
                continue

            params[field] = self._restli_list(values)
            condition.applied = True

        return params

    def _build_campaign_analytics_params(self, conditions: list[FilterCondition] | None) -> dict[str, Any]:
        time_granularity = self.DEFAULT_ANALYTICS_TIME_GRANULARITY
        start_date: date | None = None
        end_date: date | None = None
        campaign_urns: list[str] = []

        for condition in conditions or []:
            if condition.column in {"campaign_id", "id"}:
                values = self._extract_list_values(condition, prefix="urn:li:sponsoredCampaign:")
                if values:
                    campaign_urns.extend(values)
                    condition.applied = True
                continue

            if condition.column == "campaign_urn":
                values = self._extract_list_values(condition)
                if values:
                    campaign_urns.extend(values)
                    condition.applied = True
                continue

            if condition.column == "time_granularity" and condition.op == FilterOperator.EQUAL:
                candidate = str(condition.value).upper()
                if candidate in self.SUPPORTED_TIME_GRANULARITIES:
                    time_granularity = candidate
                    condition.applied = True
                continue

            if condition.column in {"date", "date_start", "start_date", "date_end", "end_date"}:
                start_date, end_date, applied = self._merge_date_condition(condition, start_date, end_date)
                if applied:
                    condition.applied = True

        today = datetime.now(timezone.utc).date()
        if end_date is None:
            end_date = today
        if start_date is None:
            start_date = end_date - timedelta(days=self.DEFAULT_ANALYTICS_LOOKBACK_DAYS - 1)
        if start_date > end_date:
            raise ValueError("LinkedIn Ads analytics start_date cannot be after end_date")

        params: dict[str, Any] = {
            "q": "analytics",
            "pivot": "CAMPAIGN",
            "timeGranularity": time_granularity,
            "dateRange": self._build_date_range_param(start_date, end_date),
            "fields": ",".join(self.CAMPAIGN_ANALYTICS_FIELDS),
        }
        if campaign_urns:
            params["campaigns"] = self._restli_list(sorted(set(campaign_urns)), encode_values=True)
        else:
            params["accounts"] = self._restli_list([f"urn:li:sponsoredAccount:{self.account_id}"], encode_values=True)
        return params

    @staticmethod
    def _extract_list_values(condition: FilterCondition, prefix: str | None = None) -> list[str] | None:
        raw_values: list[Any] | None = None
        if condition.op == FilterOperator.EQUAL:
            raw_values = [condition.value]
        elif condition.op == FilterOperator.IN and isinstance(condition.value, list):
            raw_values = condition.value

        if not raw_values:
            return None

        values = []
        for value in raw_values:
            string_value = str(value)
            if prefix and not string_value.startswith(prefix):
                string_value = f"{prefix}{string_value}"
            values.append(string_value)
        return values

    def _merge_date_condition(
        self,
        condition: FilterCondition,
        current_start: date | None,
        current_end: date | None,
    ) -> tuple[date | None, date | None, bool]:
        if condition.op == FilterOperator.BETWEEN and isinstance(condition.value, tuple) and len(condition.value) == 2:
            start_candidate = self._coerce_date(condition.value[0])
            end_candidate = self._coerce_date(condition.value[1])
            if start_candidate and end_candidate:
                return start_candidate, end_candidate, True
            return current_start, current_end, False

        value = self._coerce_date(condition.value)
        if value is None:
            return current_start, current_end, False

        if condition.column == "date":
            if condition.op == FilterOperator.EQUAL:
                return value, value, True
            if condition.op == FilterOperator.GREATER_THAN:
                return value + timedelta(days=1), current_end, True
            if condition.op == FilterOperator.GREATER_THAN_OR_EQUAL:
                return value, current_end, True
            if condition.op == FilterOperator.LESS_THAN:
                return current_start, value - timedelta(days=1), True
            if condition.op == FilterOperator.LESS_THAN_OR_EQUAL:
                return current_start, value, True
            return current_start, current_end, False

        if condition.column in {"date_start", "start_date"}:
            if condition.op == FilterOperator.EQUAL:
                return value, current_end, True
            if condition.op == FilterOperator.GREATER_THAN:
                return value + timedelta(days=1), current_end, True
            if condition.op == FilterOperator.GREATER_THAN_OR_EQUAL:
                return value, current_end, True
            return current_start, current_end, False

        if condition.column in {"date_end", "end_date"}:
            if condition.op == FilterOperator.EQUAL:
                return current_start, value, True
            if condition.op == FilterOperator.LESS_THAN:
                return current_start, value - timedelta(days=1), True
            if condition.op == FilterOperator.LESS_THAN_OR_EQUAL:
                return current_start, value, True
            return current_start, current_end, False

        return current_start, current_end, False

    @staticmethod
    def _coerce_date(value: Any) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value:
            normalized = value.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(normalized).date()
            except ValueError:
                try:
                    return date.fromisoformat(value)
                except ValueError:
                    return None
        return None

    @staticmethod
    def _build_date_range_param(start_date: date, end_date: date) -> str:
        return (
            f"(start:(year:{start_date.year},month:{start_date.month},day:{start_date.day}),"
            f"end:(year:{end_date.year},month:{end_date.month},day:{end_date.day}))"
        )

    @staticmethod
    def _restli_list(values: list[str], encode_values: bool = False) -> str:
        prepared_values = [quote(value, safe="") if encode_values else value for value in values]
        return "List(" + ",".join(prepared_values) + ")"

    @classmethod
    def _format_creative_filter_value(cls, column: str, value: Any) -> str:
        if column == "creative_id":
            return f"urn:li:sponsoredCreative:{value}"
        if column == "campaign_id":
            return f"urn:li:sponsoredCampaign:{value}"
        if column == "content_reference_id":
            return f"urn:li:share:{value}"
        return cls._format_search_value(value)

    @staticmethod
    def _extract_money(money: Any) -> tuple[Any, Any]:
        if isinstance(money, dict):
            return money.get("amount"), money.get("currencyCode")
        return None, None

    @staticmethod
    def _extract_urn_id(value: Any) -> Any:
        if isinstance(value, str) and ":" in value:
            return value.rsplit(":", 1)[-1]
        return value

    @staticmethod
    def _normalize_multi_value(value: Any) -> str | None:
        if isinstance(value, list):
            return ",".join(str(item) for item in value)
        if isinstance(value, str):
            return value
        return None

    @staticmethod
    def _normalize_date_dict(value: Any) -> str | None:
        if not isinstance(value, dict):
            return None
        year = value.get("year")
        month = value.get("month")
        day = value.get("day")
        if not isinstance(year, int) or not isinstance(month, int) or not isinstance(day, int):
            return None
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    @classmethod
    def _normalize_creative(cls, item: dict[str, Any]) -> dict[str, Any]:
        creative_urn = item.get("id")
        campaign_urn = item.get("campaign")
        account_urn = item.get("account")
        content = item.get("content") if isinstance(item.get("content"), dict) else {}
        content_reference = content.get("reference")

        return {
            "id": creative_urn,
            "creative_id": cls._extract_urn_id(creative_urn),
            "campaign_urn": campaign_urn,
            "campaign_id": cls._extract_urn_id(campaign_urn),
            "account_urn": account_urn,
            "account_id": cls._extract_urn_id(account_urn),
            "intended_status": item.get("intendedStatus"),
            "is_test": item.get("isTest"),
            "is_serving": item.get("isServing"),
            "serving_hold_reasons": cls._normalize_multi_value(item.get("servingHoldReasons")),
            "content_reference": content_reference,
            "content_reference_id": cls._extract_urn_id(content_reference),
            "created_at": item.get("createdAt"),
            "last_modified_at": item.get("lastModifiedAt"),
            "created_by": item.get("createdBy"),
            "last_modified_by": item.get("lastModifiedBy"),
        }

    @classmethod
    def _normalize_campaign_analytics(cls, item: dict[str, Any], time_granularity: str) -> dict[str, Any]:
        pivot_values = item.get("pivotValues") if isinstance(item.get("pivotValues"), list) else []
        campaign_urn = pivot_values[0] if pivot_values else None
        date_range = item.get("dateRange") if isinstance(item.get("dateRange"), dict) else {}
        start = date_range.get("start") if isinstance(date_range.get("start"), dict) else None
        end = date_range.get("end") if isinstance(date_range.get("end"), dict) else None

        return {
            "campaign_urn": campaign_urn,
            "campaign_id": cls._extract_urn_id(campaign_urn),
            "date_start": cls._normalize_date_dict(start),
            "date_end": cls._normalize_date_dict(end),
            "time_granularity": time_granularity,
            "impressions": item.get("impressions"),
            "clicks": item.get("clicks"),
            "landing_page_clicks": item.get("landingPageClicks"),
            "likes": item.get("likes"),
            "shares": item.get("shares"),
            "cost_in_local_currency": item.get("costInLocalCurrency"),
            "external_website_conversions": item.get("externalWebsiteConversions"),
        }

    @classmethod
    def _normalize_campaign(cls, item: dict[str, Any]) -> dict[str, Any]:
        daily_budget_amount, daily_budget_currency_code = cls._extract_money(item.get("dailyBudget"))
        total_budget_amount, total_budget_currency_code = cls._extract_money(item.get("totalBudget"))
        unit_cost_amount, unit_cost_currency_code = cls._extract_money(item.get("unitCost"))
        run_schedule = item.get("runSchedule") if isinstance(item.get("runSchedule"), dict) else {}
        audit = item.get("changeAuditStamps") if isinstance(item.get("changeAuditStamps"), dict) else {}
        created = audit.get("created") if isinstance(audit.get("created"), dict) else {}
        last_modified = audit.get("lastModified") if isinstance(audit.get("lastModified"), dict) else {}
        locale = item.get("locale") if isinstance(item.get("locale"), dict) else {}

        account_urn = item.get("account")
        campaign_group_urn = item.get("campaignGroup")
        return {
            "id": item.get("id"),
            "name": item.get("name"),
            "status": item.get("status"),
            "type": item.get("type"),
            "test": item.get("test"),
            "account_urn": account_urn,
            "account_id": cls._extract_urn_id(account_urn),
            "campaign_group_urn": campaign_group_urn,
            "campaign_group_id": cls._extract_urn_id(campaign_group_urn),
            "associated_entity_urn": item.get("associatedEntity"),
            "cost_type": item.get("costType"),
            "creative_selection": item.get("creativeSelection"),
            "objective_type": item.get("objectiveType"),
            "optimization_target_type": item.get("optimizationTargetType"),
            "format": item.get("format"),
            "locale_country": locale.get("country"),
            "locale_language": locale.get("language"),
            "audience_expansion_enabled": item.get("audienceExpansionEnabled"),
            "offsite_delivery_enabled": item.get("offsiteDeliveryEnabled"),
            "serving_statuses": cls._normalize_multi_value(item.get("servingStatuses")),
            "daily_budget_amount": daily_budget_amount,
            "daily_budget_currency_code": daily_budget_currency_code,
            "total_budget_amount": total_budget_amount,
            "total_budget_currency_code": total_budget_currency_code,
            "unit_cost_amount": unit_cost_amount,
            "unit_cost_currency_code": unit_cost_currency_code,
            "run_schedule_start": run_schedule.get("start"),
            "run_schedule_end": run_schedule.get("end"),
            "created_at": created.get("time"),
            "last_modified_at": last_modified.get("time"),
        }

    @classmethod
    def _normalize_campaign_group(cls, item: dict[str, Any]) -> dict[str, Any]:
        total_budget_amount, total_budget_currency_code = cls._extract_money(item.get("totalBudget"))
        daily_budget_amount, daily_budget_currency_code = cls._extract_money(item.get("dailyBudget"))
        run_schedule = item.get("runSchedule") if isinstance(item.get("runSchedule"), dict) else {}
        audit = item.get("changeAuditStamps") if isinstance(item.get("changeAuditStamps"), dict) else {}
        created = audit.get("created") if isinstance(audit.get("created"), dict) else {}
        last_modified = audit.get("lastModified") if isinstance(audit.get("lastModified"), dict) else {}

        account_urn = item.get("account")
        return {
            "id": item.get("id"),
            "name": item.get("name"),
            "status": item.get("status"),
            "test": item.get("test"),
            "account_urn": account_urn,
            "account_id": cls._extract_urn_id(account_urn),
            "run_schedule_start": run_schedule.get("start"),
            "run_schedule_end": run_schedule.get("end"),
            "serving_statuses": cls._normalize_multi_value(item.get("servingStatuses")),
            "total_budget_amount": total_budget_amount,
            "total_budget_currency_code": total_budget_currency_code,
            "daily_budget_amount": daily_budget_amount,
            "daily_budget_currency_code": daily_budget_currency_code,
            "created_at": created.get("time"),
            "last_modified_at": last_modified.get("time"),
        }
