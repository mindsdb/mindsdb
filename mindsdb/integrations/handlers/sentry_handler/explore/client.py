from __future__ import annotations

from typing import Any

from mindsdb.integrations.handlers.sentry_handler.explore.errors import (
    ExploreAuthenticationError,
    ExploreCapabilityError,
    ExplorePermissionError,
    ExploreQueryError,
)
from mindsdb.integrations.handlers.sentry_handler.explore.models import (
    ExploreTableRequest,
    ExploreTimeseriesRequest,
)
from mindsdb.integrations.handlers.sentry_handler.sentry_client import SentryClient, SentryRequestError


class ExploreClient:
    def __init__(self, *, sentry_client: SentryClient, environment: str | None = None) -> None:
        self.sentry_client = sentry_client
        self.environment = environment

    def query_table(self, request: ExploreTableRequest) -> list[dict[str, Any]]:
        payload, _ = self._request(
            "/events/",
            params=self._build_table_params(request),
            operation=f"explore {request.dataset.value} table",
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ExploreQueryError("Sentry explore table request returned malformed payload")
        return payload["data"]

    def query_timeseries(self, request: ExploreTimeseriesRequest) -> list[dict[str, Any]]:
        payload, _ = self._request(
            "/events-timeseries/",
            params=self._build_timeseries_params(request),
            operation=f"explore {request.dataset.value} timeseries",
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("timeSeries"), list):
            raise ExploreQueryError("Sentry explore timeseries request returned malformed payload")
        return payload["timeSeries"]

    def _request(
        self,
        path: str,
        *,
        params: dict[str, Any],
        operation: str,
    ) -> tuple[Any, Any]:
        try:
            return self.sentry_client.request_json(
                "GET",
                f"/organizations/{self.sentry_client.organization_slug}{path}",
                params=params,
                operation=operation,
            )
        except SentryRequestError as exc:
            if exc.status_code == 401:
                raise ExploreAuthenticationError(str(exc)) from exc
            if exc.status_code == 403:
                raise ExplorePermissionError(str(exc)) from exc
            if exc.status_code == 404:
                raise ExploreCapabilityError(str(exc)) from exc
            raise ExploreQueryError(str(exc)) from exc

    def _build_table_params(self, request: ExploreTableRequest) -> dict[str, Any]:
        params = self._build_common_params(
            dataset=request.dataset.value,
            project_ids=request.project_ids,
            environments=request.environments,
            start=request.start,
            end=request.end,
            stats_period=request.stats_period,
            query=request.query,
        )
        params["field"] = request.fields
        params["per_page"] = request.limit
        if request.sort:
            params["sort"] = request.sort
        return params

    def _build_timeseries_params(self, request: ExploreTimeseriesRequest) -> dict[str, Any]:
        params = self._build_common_params(
            dataset=request.dataset.value,
            project_ids=request.project_ids,
            environments=request.environments,
            start=request.start,
            end=request.end,
            stats_period=request.stats_period,
            query=request.query,
        )
        params["yAxis"] = request.y_axis
        params["interval"] = request.interval
        return params

    @staticmethod
    def _build_common_params(
        *,
        dataset: str,
        project_ids: list[int],
        environments: list[str],
        start: str | None,
        end: str | None,
        stats_period: str | None,
        query: str,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "dataset": dataset,
            "project": project_ids,
        }
        if environments:
            params["environment"] = environments
        if query:
            params["query"] = query
        if stats_period:
            params["statsPeriod"] = stats_period
        else:
            if start:
                params["start"] = start
            if end:
                params["end"] = end
        return params
