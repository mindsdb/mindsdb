from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from mindsdb.integrations.handlers.sentry_handler.explore.models import (
    ExploreDataset,
    ExploreTableRequest,
    ExploreTimeseriesRequest,
)
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn


DEFAULT_STATS_PERIOD = "7d"
LOG_TABLE_COLUMNS = [
    "timestamp",
    "level",
    "message",
    "trace_id",
    "span_id",
    "release",
    "environment",
    "project_id",
    "project_slug",
    "logger",
    "extra_json",
]
LOG_FIELD_MAP = {
    "timestamp": "timestamp",
    "level": "severity",
    "message": "message",
    "trace_id": "trace.id",
    "span_id": "span.id",
    "release": "sentry.release",
    "logger": "logger.name",
}
LOG_QUERY_KEY_MAP = {
    "level": "severity",
    "trace_id": "trace.id",
    "span_id": "span.id",
    "release": "sentry.release",
    "logger": "logger.name",
}
TIMESERIES_COLUMNS = ["bucket_start", "value"]


def build_logs_request(
    *,
    project_id: int,
    environment: str | None,
    conditions: list[FilterCondition],
    limit: int | None,
    sort: list[SortColumn] | None,
    targets: list[str] | None,
) -> ExploreTableRequest:
    request_limit = 100 if limit is None else min(int(limit), 100)
    start, end, stats_period = _resolve_window(conditions, time_columns={"timestamp"})
    query = _build_query_string(conditions)
    request_sort = _resolve_logs_sort(sort)
    request_fields = _resolve_logs_fields(targets)

    return ExploreTableRequest(
        dataset=ExploreDataset.LOGS,
        fields=request_fields,
        query=query,
        limit=request_limit,
        sort=request_sort,
        start=start,
        end=end,
        stats_period=stats_period,
        project_ids=[project_id],
        environments=[environment] if environment else [],
    )


def build_logs_timeseries_request(
    *,
    project_id: int,
    environment: str | None,
    conditions: list[FilterCondition],
) -> ExploreTimeseriesRequest:
    start, end, stats_period = _resolve_window(conditions, time_columns={"bucket_start", "timestamp"})
    query = _build_query_string(conditions)
    interval = _resolve_interval_seconds(start=start, end=end, stats_period=stats_period)

    return ExploreTimeseriesRequest(
        dataset=ExploreDataset.LOGS,
        query=query,
        y_axis="count()",
        interval=interval,
        start=start,
        end=end,
        stats_period=stats_period,
        project_ids=[project_id],
        environments=[environment] if environment else [],
    )


def _resolve_logs_fields(targets: list[str] | None) -> list[str]:
    if not targets:
        requested_columns = LOG_TABLE_COLUMNS
    else:
        requested_columns = [target for target in targets if target in LOG_TABLE_COLUMNS]

    fields = [LOG_FIELD_MAP[column] for column in requested_columns if column in LOG_FIELD_MAP]
    return fields or ["timestamp"]


def _resolve_logs_sort(sort: list[SortColumn] | None) -> str:
    if sort:
        primary_sort = sort[0]
        field = LOG_FIELD_MAP.get(primary_sort.column)
        if field:
            primary_sort.applied = True
            return field if primary_sort.ascending else f"-{field}"
    return "-timestamp"


def _resolve_window(
    conditions: list[FilterCondition],
    *,
    time_columns: set[str],
) -> tuple[str | None, str | None, str | None]:
    start_value: pd.Timestamp | None = None
    end_value: pd.Timestamp | None = None

    for condition in conditions:
        if condition.column not in time_columns:
            continue

        timestamp = pd.to_datetime(condition.value, utc=True, errors="coerce")
        if pd.isna(timestamp):
            raise ValueError(f"Unsupported where value for {condition.column}: {condition.value}")

        normalized_timestamp = timestamp.to_pydatetime()

        if condition.op in {FilterOperator.EQUAL, FilterOperator.GREATER_THAN_OR_EQUAL, FilterOperator.GREATER_THAN}:
            if condition.op == FilterOperator.GREATER_THAN:
                normalized_timestamp = normalized_timestamp + timedelta(microseconds=1)
            start_value = _max_timestamp(start_value, normalized_timestamp)
            condition.applied = True

        if condition.op in {FilterOperator.EQUAL, FilterOperator.LESS_THAN_OR_EQUAL, FilterOperator.LESS_THAN}:
            if condition.op == FilterOperator.LESS_THAN:
                normalized_timestamp = normalized_timestamp - timedelta(microseconds=1)
            end_value = _min_timestamp(end_value, normalized_timestamp)
            condition.applied = True

    if start_value is None and end_value is None:
        return None, None, DEFAULT_STATS_PERIOD

    if start_value and end_value and start_value > end_value:
        raise ValueError("Invalid timestamp filter range for Sentry Explore request")

    return _isoformat(start_value), _isoformat(end_value), None


def _build_query_string(conditions: list[FilterCondition]) -> str:
    query_parts: list[str] = []

    for condition in conditions:
        if condition.applied:
            continue

        if condition.column == "message":
            query_parts.append(_build_message_query(condition))
            condition.applied = True
            continue

        query_key = LOG_QUERY_KEY_MAP.get(condition.column)
        if query_key is None:
            continue

        query_parts.append(_build_keyed_query(query_key, condition))
        condition.applied = True

    return " ".join(part for part in query_parts if part)


def _build_message_query(condition: FilterCondition) -> str:
    if condition.op == FilterOperator.EQUAL:
        return _quote_value(condition.value)
    if condition.op == FilterOperator.LIKE:
        return _like_pattern_to_search(condition.value)
    if condition.op == FilterOperator.IN:
        return " OR ".join(_quote_value(value) for value in condition.value)
    raise ValueError(f"Unsupported where operation for message: {condition.op.value}")


def _build_keyed_query(query_key: str, condition: FilterCondition) -> str:
    if condition.op == FilterOperator.EQUAL:
        return f"{query_key}:{_quote_value(condition.value)}"
    if condition.op == FilterOperator.IN:
        values = ",".join(_quote_value(value) for value in condition.value)
        return f"{query_key}:[{values}]"
    if condition.op == FilterOperator.LIKE:
        return f"{query_key}:{_like_pattern_to_search(condition.value)}"
    raise ValueError(f"Unsupported where operation for {condition.column}: {condition.op.value}")


def _like_pattern_to_search(value: object) -> str:
    pattern = str(value).replace("%", "*").replace("_", "?")
    if "*" not in pattern and "?" not in pattern:
        return _quote_value(pattern)
    return pattern


def _quote_value(value: object) -> str:
    raw_value = str(value).replace('"', '\\"')
    if any(char.isspace() for char in raw_value):
        return f'"{raw_value}"'
    return raw_value


def _resolve_interval_seconds(*, start: str | None, end: str | None, stats_period: str | None) -> int:
    if stats_period:
        window_seconds = _stats_period_to_seconds(stats_period)
    elif start and end:
        window_seconds = max(int((pd.to_datetime(end, utc=True) - pd.to_datetime(start, utc=True)).total_seconds()), 0)
    elif start:
        window_seconds = max(
            int((datetime.now(tz=timezone.utc) - pd.to_datetime(start, utc=True).to_pydatetime()).total_seconds()),
            0,
        )
    elif end:
        window_seconds = 7 * 24 * 60 * 60
    else:
        window_seconds = 7 * 24 * 60 * 60

    return 3600 if window_seconds <= 48 * 60 * 60 else 86400


def _stats_period_to_seconds(stats_period: str) -> int:
    unit = stats_period[-1]
    amount = int(stats_period[:-1])
    factors = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
    }
    if unit not in factors:
        raise ValueError(f"Unsupported stats period unit: {stats_period}")
    return amount * factors[unit]


def _max_timestamp(current: datetime | None, candidate: datetime) -> datetime:
    if current is None or candidate > current:
        return candidate
    return current


def _min_timestamp(current: datetime | None, candidate: datetime) -> datetime:
    if current is None or candidate < current:
        return candidate
    return current


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()
